package com.adamroughton.concentus.crowdhammer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.util.RunningStats;
import com.esotericsoftware.minlog.Log;

public class ResultExtractor {
	
	public static void main(String[] args) {
		if (args.length < 2) printUsage();
		
		try {
			Path dbPath = Paths.get(args[0]);
			if (!Files.exists(dbPath)) printUsage("Could not find file " + dbPath.toString());
			
			Path requestedCsvPath = Paths.get(args[1]);
			if (!requestedCsvPath.isAbsolute()) {
				requestedCsvPath = Paths.get(System.getProperty("user.dir"), args[1]);
			}
			Path csvPath = createUniqueFile(requestedCsvPath);
			
			try {
				Class.forName("org.sqlite.JDBC");
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(String.format("The driver for handling the sqlite database " +
						"not found: '%s'", e.getMessage()), e);
			}
			
			Connection connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", dbPath.toString()));
			Statement statement = connection.createStatement();
			
			// get the tests
			Log.info("Looking up tests in the database...");
			Map<String, TestInfo> testLookup = new HashMap<>();
			if (!statement.execute("select r.runId as runId, r.name as name, r.clientCount as clientCount, " +
					"d.count as workerCount from RunMetaData as r inner " +
					"join RunDeploymentData as d on r.runId = d.runId and d.serviceType = \"worker\"")) {
				System.out.println("No tests found in the data file!");
				System.exit(0);
			}
			try (ResultSet resultSet = statement.getResultSet()) {
				while (resultSet.next()) {
					String testName = resultSet.getString("name");
					TestInfo test;
					if (!testLookup.containsKey(testName)) {
						Log.info("Found test: " + testName);
						int workerCount = resultSet.getInt("workerCount");
						test = new TestInfo(testName, null, workerCount);
						testLookup.put(testName, test);
					} else {
						test = testLookup.get(testName);
					}
					int runId = resultSet.getInt("runId");
					int clientCount = resultSet.getInt("clientCount");
					test.runs.add(new TestRunInfo(runId, clientCount));
				}
			}
			
			PreparedStatement countMetricStatement = connection.prepareStatement(
					"select c.bucketId as bucketId, c.sourceId as sourceId, c.count as count, c.bucketDuration as duration " +
							"from CountMetric as c " +
					"inner join SourceData as s on c.sourceId = s.sourceId " +
					"inner join MetricMetaData m on c.metricId = m.metricId " +
					"and c.sourceId = m.sourceId  " +
					"where s.serviceType = ? and m.reference = ? and m.name = ? and c.runId = ?" +
					"order by c.bucketId, c.sourceId asc");
			PreparedStatement statsMetricStatement = connection.prepareStatement(
					"select stat.bucketId as bucketId, stat.sourceId as sourceId, stat.mean as mean, stat.sumSqrs as sumSqrs, " +
							"stat.count as count, stat.min as min, stat.max as max from StatsMetric as stat " +
					"inner join SourceData as s on stat.sourceId = s.sourceId " +
					"inner join MetricMetaData m on stat.metricId = m.metricId " +
					"and stat.sourceId = m.sourceId  " +
					"where s.serviceType = ? and m.reference = ? and m.name = ? and stat.runId = ?" +
					"order by stat.bucketId, stat.sourceId asc");
			
			Log.info("Starting result collection");
			try (BufferedWriter resultFileWriter = Files.newBufferedWriter(csvPath, StandardCharsets.ISO_8859_1, StandardOpenOption.APPEND)) {	
				resultFileWriter.write(TestRunResultSet.getCSVStringHeader() + '\n');
				for (TestInfo test : testLookup.values()) {
					Log.info("Collecting for test " + test.testName + ", deployment " + test.deploymentName);
					for (TestRunInfo run : test) {
						Log.info("Collecting for run " + run.runId);
						final TestRunResultSet testRunResultSet = new TestRunResultSet(test, run);
						
						// get the connected client counts
						collectMetric(countMetricStatement, test, run, "connectedClientCount", new AggregateDelegate() {
								
								int partialCount = 0;
								
								@Override
								public void push(ResultSet resultSet, boolean isLastRecordForBucketId) throws SQLException {
									int connectedClientCount = resultSet.getInt("count");
									partialCount += connectedClientCount;
									if (isLastRecordForBucketId) {
										testRunResultSet.connectedClientCount.push(partialCount);
										partialCount = 0;
									}
								}
							});
						
						// get the sentActionThroughput
						collectMetric(countMetricStatement, test, run, "sentActionThroughput", new AggregateDelegate() {
							
							int partialCount = 0;
							long bucketDuration = -1;
							
							@Override
							public void push(ResultSet resultSet, boolean isLastRecordForBucketId) throws SQLException {
								if (bucketDuration == -1) {
									bucketDuration = resultSet.getLong("duration");
								}
								
								int sentActionCount = resultSet.getInt("count");
								partialCount += sentActionCount;
								if (isLastRecordForBucketId) {
									// work out throughput
									double throughput = (double) partialCount / bucketDuration * TimeUnit.SECONDS.toMillis(1);
									testRunResultSet.sentActionThroughput.push(throughput);
									partialCount = 0;
									bucketDuration = -1;
								}
							}
						});
						
						// get the action to canonical state latency
						collectMetric(statsMetricStatement, test, run, "actionToCanonicalStateLatency", new AggregateDelegate() {
								
								@Override
								public void push(ResultSet resultSet, boolean isLastRecordForBucketId) throws SQLException {
									int count = resultSet.getInt("count");
									double mean = resultSet.getDouble("mean");
									double sumSqrs = resultSet.getDouble("sumSqrs");
									double min = resultSet.getDouble("min");
									double max = resultSet.getDouble("max");
									testRunResultSet.actionToCanonicalStateLatency.merge(count, mean, sumSqrs, min, max);
								}
							});
						
						// get the lateActionToCanonicalStateLatency
						collectMetric(countMetricStatement, test, run, "lateActionToCanonicalStateCount", new AggregateDelegate() {
							
							@Override
							public void push(ResultSet resultSet, boolean isLastRecordForBucketId) throws SQLException {
								int lateCount = resultSet.getInt("count");
								testRunResultSet.lateActionToCanonicalStateLatency += lateCount;
							}
						});
						
						// write the run to the result file
						resultFileWriter.write(testRunResultSet.toCSVString() + '\n');
					}	
					
				}
			}
			Log.info("Done! Results written to " + csvPath.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static interface AggregateDelegate {
		void push(ResultSet resultSet, boolean isLastRecordForBucketId) throws SQLException;
	}
	
	private static void collectMetric(PreparedStatement statement, TestInfo test, 
			TestRunInfo run, String metricName, AggregateDelegate aggregateDelegate) throws SQLException {
		statement.setString(1, "worker");
		statement.setString(2, "clientProcessor");
		statement.setString(3, metricName);
		statement.setInt(4, run.runId);
		if (!statement.execute()) {
			Log.warn("No data found for metric " + metricName + " on run " + run.runId + " in test " + test.testName + "!");
			return;
		}
		
		try (ResultSet resultSet = statement.getResultSet()) {
			aggregate(test, resultSet, aggregateDelegate);
		}
	}
	
	private static void aggregate(TestInfo test, ResultSet resultSet, 
			AggregateDelegate aggregateDelegate) throws SQLException {
		long currentBucketId = -1;
		int recordCountForBucket = 0;
		while (resultSet.next()) {
			int bucketId = resultSet.getInt("bucketId");
			if (currentBucketId == -1) {
				currentBucketId = bucketId;
				recordCountForBucket = 1;
				aggregateDelegate.push(resultSet, false);
			} else if (currentBucketId != bucketId) {
				if (recordCountForBucket != test.workerCount) {
					Log.warn("Expected " + test.workerCount + " buckets, but only got " 
							+ recordCountForBucket + " for bucketId " + currentBucketId);
				}
				if (bucketId != currentBucketId + 1) {
					Log.warn("Missing bucket! Expected " + (currentBucketId + 1) + ", but got " + bucketId);
				}
				aggregateDelegate.push(resultSet, true);
				currentBucketId = bucketId;
				recordCountForBucket = 1;
			} else {
				recordCountForBucket++;
				aggregateDelegate.push(resultSet, false);
			}
		}
	}
	
	private static void printUsage() {
		printUsage(null);
	}
	
	private static void printUsage(String message) {
		System.out.println((message != null && message.length() != 0? message + "\n":"") + "Usage:\nResultExtractor <sqlite file path> <output csv file>");
		System.exit(1);
	}
	
	private static Path createUniqueFile(Path requestedPath) throws IOException {
		if (!requestedPath.isAbsolute() || Files.isDirectory(requestedPath)) 
			throw new IllegalArgumentException("The path must resolve to a file");
		
		String fileName = requestedPath.getFileName().toString();
		String fileNameBase;
		String suffix;
		int extDelimIndex = fileName.lastIndexOf('.');
		if (extDelimIndex != -1) {
			suffix = fileName.substring(extDelimIndex + 1);
			fileNameBase = fileName.substring(0, extDelimIndex);
		} else {
			fileNameBase = fileName;
			suffix = "";
		}
		Path basePath = requestedPath.getParent();
		
		Path finalPath = null;
		int appendixNum = 0;
		do {
			try {
				Path attemptPath;
				if (appendixNum > 0) {
					attemptPath = basePath.resolve(fileNameBase + Integer.toString(appendixNum) + "." + suffix);
				} else {
					attemptPath = basePath.resolve(fileNameBase + "." + suffix);	
				}
				finalPath = Files.createFile(attemptPath);
			} catch (FileAlreadyExistsException eAlreadyExists) {
				appendixNum++;
			}
		} while (finalPath == null);
		
		return finalPath;
	}
	
	private static class TestInfo implements Iterable<TestRunInfo> {
		public final String testName;
		public final String deploymentName;
		private final int workerCount;
		private final ArrayList<TestRunInfo> runs = new ArrayList<>();
		
		public TestInfo(String testName, String deploymentName, int workerCount) {
			this.testName = testName;
			this.deploymentName = deploymentName;
			this.workerCount = workerCount;
		}
		
		@Override
		public Iterator<TestRunInfo> iterator() {
			Collections.sort(runs);
			return runs.iterator();
		}
	}
	
	private static class TestRunInfo implements Comparable<TestRunInfo> {
		public final int runId;
		public final int clientCount;
		
		public TestRunInfo(int runId, int clientCount) {
			this.runId = runId;
			this.clientCount = clientCount;
		}

		@Override
		public int compareTo(TestRunInfo that) {
			if (this.runId != that.runId) {
				return this.runId - that.runId;
			} else {
				return this.clientCount - that.clientCount;
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + clientCount;
			result = prime * result + runId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof TestRunInfo)) {
				return false;
			}
			TestRunInfo other = (TestRunInfo) obj;
			if (clientCount != other.clientCount) {
				return false;
			}
			if (runId != other.runId) {
				return false;
			}
			return true;
		}
		
	}
	
	private static class TestRunResultSet {
		public final TestInfo test;
		public final TestRunInfo run;
		public final RunningStats connectedClientCount = new RunningStats();
		public final RunningStats sentActionThroughput = new RunningStats();
		public final RunningStats actionToCanonicalStateLatency = new RunningStats();
		public int lateActionToCanonicalStateLatency = 0;
		
		public TestRunResultSet(TestInfo test, TestRunInfo run) {
			this.test = Objects.requireNonNull(test);
			this.run = Objects.requireNonNull(run);
		}
		
		public static String getCSVStringHeader() {
			StringBuilder strBuilder = new StringBuilder()
				.append("testName").append(",")
				.append("deploymentName").append(",")
				.append("clientCount").append(",");
			appendStatsHeader(strBuilder, "connectedClientCount").append(",");
			appendStatsHeader(strBuilder, "sentActionThroughput").append(",");
			appendStatsHeader(strBuilder, "actionToCanonicalStateLatency").append(",")
				.append("lateActionToCanonicalStateLatency");
			return strBuilder.toString();
		}
		
		private static StringBuilder appendStatsHeader(StringBuilder strBuilder, String metricName) {
			return strBuilder
					.append(metricName).append("_mean").append(",")
					.append(metricName).append("_stdDev").append(",")
					.append(metricName).append("_min").append(",")
					.append(metricName).append("_max").append(",")
					.append(metricName).append("_count").append(",")
					.append(metricName).append("_sumSqrs");
		}
		
		public String toCSVString() {
			StringBuilder strBuilder = new StringBuilder();
			strBuilder
				.append(test.testName).append(",")
				.append(test.deploymentName).append(",")
				.append(run.clientCount).append(",");
			appendAsCSVString(strBuilder, connectedClientCount).append(",");
			appendAsCSVString(strBuilder, sentActionThroughput).append(",");
			appendAsCSVString(strBuilder, actionToCanonicalStateLatency).append(",")
				.append(lateActionToCanonicalStateLatency);
			return strBuilder.toString();
		}
		
		private StringBuilder appendAsCSVString(StringBuilder strBuilder, RunningStats stats) {
			return strBuilder
				.append(stats.getMean()).append(",")
				.append(stats.getStandardDeviation()).append(",")
				.append(stats.getMin()).append(",")
				.append(stats.getMax()).append(",")
				.append(stats.getCount()).append(",")
				.append(stats.getSumOfSquares());
		}
	}
	
}
