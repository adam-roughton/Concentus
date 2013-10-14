package com.adamroughton.concentus.crowdhammer;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
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

import static com.adamroughton.concentus.util.Util.createUniqueFile;

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
			if (!statement.execute("select r.runId as runId, r.name as name, r.deploymentName as deployment, r.clientCount as clientCount, " +
					"d.count as workerCount from RunMetaData as r inner " +
					"join RunDeploymentData as d on r.runId = d.runId and d.serviceType = \"worker\"" +
					"order by r.runId")) {
				System.out.println("No tests found in the data file!");
				System.exit(0);
			}
			try (ResultSet resultSet = statement.getResultSet()) {
				while (resultSet.next()) {
					String testName = resultSet.getString("name");
					String deployment = resultSet.getString("deployment");
					String runName = testName + deployment;
					TestInfo test;
					if (!testLookup.containsKey(runName)) {
						Log.info("Found test: " + testName);
						int workerCount = resultSet.getInt("workerCount");
						test = new TestInfo(testName, deployment, workerCount);
						testLookup.put(runName, test);
					} else {
						test = testLookup.get(runName);
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
						Log.info("Collecting client count stats...");
						collectWorkerMetric(countMetricStatement, test, run, "connectedClientCount", new AggregateDelegate() {
								
								int partialCount = 0;
								boolean hasPartial = false;
								
								@Override
								public void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException {
									if (isNewBucketId && hasPartial) {
										testRunResultSet.bucketCount++; // collect just for the first metric (remaining will have the same count)
										testRunResultSet.connectedClientCount.push(partialCount);
										partialCount = 0;
										hasPartial = false;
									}
									int connectedClientCount = resultSet.getInt("count");
									partialCount += connectedClientCount;
									hasPartial = true;
								}

								@Override
								public void dropCurrentBucket() {
									partialCount = 0;
									hasPartial = false;
								}
							});
						
						// get the sentActionThroughput
						collectWorkerMetric(countMetricStatement, test, run, "sentActionThroughput", new AggregateDelegate() {
							
							int partialCount = 0;
							long bucketDuration = -1;
							
							@Override
							public void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException {
								if (isNewBucketId && bucketDuration != -1) {
									// work out throughput
									double throughput = (double) partialCount / bucketDuration * TimeUnit.SECONDS.toMillis(1);
									testRunResultSet.sentActionThroughput.push(throughput);
									partialCount = 0;
									bucketDuration = -1;
								}
								
								if (bucketDuration == -1) {
									bucketDuration = resultSet.getLong("duration");
								}
								int sentActionCount = resultSet.getInt("count");
								partialCount += sentActionCount;
							}

							@Override
							public void dropCurrentBucket() {
								partialCount = 0;
								bucketDuration = -1;
							}
						});
						
						// get the action to canonical state latency
						collectWorkerMetric(statsMetricStatement, test, run, "actionToCanonicalStateLatency", new AggregateDelegate() {
								
							private RunningStats partial = new RunningStats();
							
							@Override
							public void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException {
								if (isNewBucketId && partial.getCount() > 0) {
									testRunResultSet.actionToCanonicalStateLatency.merge(partial);
									partial.reset();
								}
								int count = resultSet.getInt("count");
								double mean = resultSet.getDouble("mean");
								double sumSqrs = resultSet.getDouble("sumSqrs");
								double min = resultSet.getDouble("min");
								double max = resultSet.getDouble("max");
								partial.merge(count, mean, sumSqrs, min, max);
							}

							@Override
							public void dropCurrentBucket() {
								partial.reset();
							}
								
						});
						
						// get the lateActionToCanonicalStateLatency
						collectWorkerMetric(countMetricStatement, test, run, "lateActionToCanonicalStateCount", new AggregateDelegate() {
							
							private int partialCount = -1;
							
							@Override
							public void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException {
								if (isNewBucketId && partialCount > -1) {
									testRunResultSet.lateActionToCanonicalStateLatency += partialCount;
									partialCount = -1;
								}
								int lateCount = resultSet.getInt("count");
								if (partialCount == -1) {
									partialCount = 0;
								}
								partialCount += lateCount;
							}

							@Override
							public void dropCurrentBucket() {
								partialCount = -1;
							}
						});
						
						/*
						 * For SingleDisruptor based approaches, we can also collect some aggregation statistics.
						 */
						collectMetric(statsMetricStatement, "canonical_state", "directStateProcessor", test, run, "aggregationMillis", new AggregateDelegate() {

							private RunningStats partial = new RunningStats();
							
							@Override
							public void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException {
								if (isNewBucketId && partial.getCount() > 0) {
									testRunResultSet.aggregationMillis.merge(partial);
									partial.reset();
								}
								int count = resultSet.getInt("count");
								double mean = resultSet.getDouble("mean");
								double sumSqrs = resultSet.getDouble("sumSqrs");
								double min = resultSet.getDouble("min");
								double max = resultSet.getDouble("max");
								partial.merge(count, mean, sumSqrs, min, max);
							}

							@Override
							public void dropCurrentBucket() {
								partial.reset();
							}
							
						});
						
						collectMetric(statsMetricStatement, "canonical_state", "directStateProcessor", test, run, "aggregationBatchLength", new AggregateDelegate() {

							private RunningStats partial = new RunningStats();
							
							@Override
							public void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException {
								if (isNewBucketId && partial.getCount() > 0) {
									testRunResultSet.aggregationBatchLength.merge(partial);
									partial.reset();
								}
								int count = resultSet.getInt("count");
								double mean = resultSet.getDouble("mean");
								double sumSqrs = resultSet.getDouble("sumSqrs");
								double min = resultSet.getDouble("min");
								double max = resultSet.getDouble("max");
								partial.merge(count, mean, sumSqrs, min, max);
							}

							@Override
							public void dropCurrentBucket() {
								partial.reset();
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
		void push(ResultSet resultSet, boolean isNewBucketId) throws SQLException;
		void dropCurrentBucket();
	}
	
	private static void collectWorkerMetric(PreparedStatement statement, TestInfo test, 
			TestRunInfo run, String metricName, AggregateDelegate aggregateDelegate) throws SQLException {
		collectMetric(statement, "worker", "clientProcessor", test, run, metricName, aggregateDelegate);
	}
	
	private static void collectMetric(PreparedStatement statement, String serviceType, String reference, TestInfo test,
			TestRunInfo run, String metricName, AggregateDelegate aggregateDelegate) throws SQLException {
		statement.setString(1, serviceType);
		statement.setString(2, reference);
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
					aggregateDelegate.dropCurrentBucket();
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
		public final RunningStats aggregationMillis = new RunningStats();
		public final RunningStats aggregationBatchLength = new RunningStats();
		public int lateActionToCanonicalStateLatency = 0;
		public int bucketCount = 0;
		
		public TestRunResultSet(TestInfo test, TestRunInfo run) {
			this.test = Objects.requireNonNull(test);
			this.run = Objects.requireNonNull(run);
		}
		
		public static String getCSVStringHeader() {
			StringBuilder strBuilder = new StringBuilder()
				.append("testName").append(",")
				.append("deploymentName").append(",")
				.append("bucketCount").append(",")
				.append("clientCount").append(",");
			appendStatsHeader(strBuilder, "connectedClientCount").append(",");
			appendStatsHeader(strBuilder, "sentActionThroughput").append(",");
			appendStatsHeader(strBuilder, "actionToCanonicalStateLatency").append(",")
				.append("lateActionToCanonicalStateLatency").append(",");
			appendStatsHeader(strBuilder, "aggregationMillis").append(",");
			appendStatsHeader(strBuilder, "aggregationBatchLength");
				
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
				.append(bucketCount).append(",")
				.append(run.clientCount).append(",");
			appendAsCSVString(strBuilder, connectedClientCount).append(",");
			appendAsCSVString(strBuilder, sentActionThroughput).append(",");
			appendAsCSVString(strBuilder, actionToCanonicalStateLatency).append(",")
				.append(lateActionToCanonicalStateLatency).append(",");
			appendAsCSVString(strBuilder, aggregationMillis).append(",");
			appendAsCSVString(strBuilder, aggregationBatchLength);
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
