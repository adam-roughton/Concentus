package com.adamroughton.concentus.crowdhammer.metriccollector;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.javatuples.Pair;

import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.util.RunningStats;

public class SqliteMetricStore implements MetricStore {

	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(String.format("The driver for handling the sqlite database " +
					"for metric results was not found: '%s'", e.getMessage()), e);
		}
	}
	
	private final Connection _connection;
	private boolean _isClosed = false;
	
	private final PreparedStatement _insertRunMetaDataStatement;
	private final PreparedStatement _insertRunDeploymentDataStatement;
	private final PreparedStatement _insertSourceDataStatement;
	private final PreparedStatement _insertMetricMetaDataStatement;
	private final PreparedStatement _insertCountMetricStatement;
	private final PreparedStatement _insertStatsMetricStatement;
	private final List<PreparedStatement> _statements = new ArrayList<>();
	
	public SqliteMetricStore(Path databasePath) {
		try {
			_connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", databasePath.toString()));
			prepareTables();
			_connection.setAutoCommit(false);
			_insertRunMetaDataStatement = newPreparedStatement("insert into RunMetaData values (?, ?, ?, ?, ?, ?, ?)");
			_insertRunDeploymentDataStatement = newPreparedStatement("insert into RunDeploymentData values (?, ?, ?)");
			_insertSourceDataStatement = newPreparedStatement("insert into SourceData values (?, ?, ?, ?)");
			_insertMetricMetaDataStatement = newPreparedStatement("insert into MetricMetaData values (?, ?, ?, ?, ?, ?)");
			_insertCountMetricStatement = newPreparedStatement("insert into CountMetric values (?, ?, ?, ?, ?, ?, ?)");
			_insertStatsMetricStatement = newPreparedStatement("insert into StatsMetric values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		} catch (SQLException eSql) {
			throw new RuntimeException(String.format("Unable to open the database file '%s'", databasePath.toString()), eSql);
		}
	}
	
	private PreparedStatement newPreparedStatement(String sql) throws SQLException {
		PreparedStatement statement = _connection.prepareStatement(sql);
		_statements.add(statement);
		return statement;
	}
	
	/*
	 * ========
	 * 	Tables:
	 * ========
	 * 
	 * 	RunMetaData:
	 * 		| runId (INT) | name (TEXT) | clientCount (INT) | duration (BIGINT) | applicationClass (TEXT) | deploymentName (TEXT) | agentClass (TEXT)
	 * 
	 *  RunDeploymentData:
	 *      | runId (INT) | serviceType (TEXT) | count (INT)
	 * 
	 * 	SourceData:
	 * 		| runId (INT) | sourceID (INT) | name (TEXT) | serviceType (TEXT)
	 * 
	 * 	MetricMetaData:
	 * 		| runId (INT) | sourceId (INT) | metricId (INT) | isCumulative (BOOLEAN) | reference (TEXT) | name (TEXT) |
	 * 
	 * 	CountMetric:
	 * 		| runId (INT) | sourceId (INT) | metricId (INT) | bucketId (BIGINT) | bucketDuration (BIGINT) | isThroughput (BOOLEAN) | count (BIGINT) |
	 * 
	 * 	StatsMetric:
	 * 		| runId (INT) | sourceId (INT) | metricId (INT) | bucketId (BIGINT) | bucketDuration (BIGINT) | mean (DOUBLE) | stddev (DOUBLE) | 
	 * 			sumSqrs (DOUBLE) | count (INT) | min (DOUBLE) | max (DOUBLE) |
	 * 
	 */
	
	private void prepareTables() throws SQLException {
		Statement prepareTablesStatement = _connection.createStatement();
		prepareTablesStatement.setQueryTimeout(30);
		prepareTablesStatement.executeUpdate("create table if not exists RunMetaData(runId INT, name TEXT, clientCount INT, duration BIGINT, " +
				"applicationClass TEXT, deploymentName TEXT, agentClass TEXT)");
		prepareTablesStatement.executeUpdate("create table if not exists RunDeploymentData(runId INT, serviceType TEXT, count INT)");
		prepareTablesStatement.executeUpdate("create table if not exists SourceData(runId INT, sourceId INT, name TEXT, serviceType TEXT)");
		prepareTablesStatement.executeUpdate("create table if not exists MetricMetaData(runId INT, sourceId INT, metricId INT, " +
				"isCumulative BOOLEAN, reference TEXT, name TEXT)");
		prepareTablesStatement.executeUpdate("create table if not exists CountMetric (runId INT, sourceId INT, metricId INT, " +
				"bucketId BIGINT, bucketDuration BIGINT, isThroughput BOOLEAN, count BIGINT)");
		prepareTablesStatement.executeUpdate("create table if not exists StatsMetric (runId INT, sourceId INT, metricId INT, " +
				"bucketId BIGINT, bucketDuration BIGINT, mean DOUBLE, stddev DOUBLE, sumSqrs DOUBLE, count INT, min DOUBLE, max DOUBLE)");
	}
	
	@Override
	public void close() throws IOException {
		try {
			_connection.close();
		} catch (SQLException e) {
			throw new IOException(e);
		} finally {
			_isClosed = true;
		}
	}

	@Override
	public void pushTestRunMetaData(int runId, String name, int clientCount, 
			long durationMillis, Class<? extends CollectiveApplication> applicationClass, 
			String deploymentName, Class<? extends ClientAgent> agentClass, Set<Pair<String, Integer>> deploymentInfo) {
		
		try {
			_insertRunMetaDataStatement.setInt(1, runId);
			_insertRunMetaDataStatement.setString(2, name);
			_insertRunMetaDataStatement.setInt(3, clientCount);
			_insertRunMetaDataStatement.setLong(4, durationMillis);
			_insertRunMetaDataStatement.setString(5, applicationClass.getName());
			_insertRunMetaDataStatement.setString(6, deploymentName);
			_insertRunMetaDataStatement.setString(7, agentClass.getName());
			_insertRunMetaDataStatement.execute();
			
			for (Pair<String, Integer> deployment : deploymentInfo) {
				_insertRunDeploymentDataStatement.setInt(1, runId);
				_insertRunDeploymentDataStatement.setString(2, deployment.getValue0());
				_insertRunDeploymentDataStatement.setInt(3, deployment.getValue1());
				_insertRunDeploymentDataStatement.execute();
			}
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}
	
	@Override
	public void pushSourceMetaData(int runId, int sourceId, String name, String serviceType) {
		try {
			_insertSourceDataStatement.setInt(1, runId);
			_insertSourceDataStatement.setInt(2, sourceId);
			_insertSourceDataStatement.setString(3, name);
			_insertSourceDataStatement.setString(4, serviceType);
			_insertSourceDataStatement.execute();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}

	@Override
	public void pushMetricMetaData(int runId, int sourceId, int metricId,
			String reference, String metricName, boolean isCumulative) {
		try {
			_insertMetricMetaDataStatement.setInt(1, runId);
			_insertMetricMetaDataStatement.setInt(2, sourceId);
			_insertMetricMetaDataStatement.setInt(3, metricId);
			_insertMetricMetaDataStatement.setBoolean(4, isCumulative);
			_insertMetricMetaDataStatement.setString(5, reference);
			_insertMetricMetaDataStatement.setString(6, metricName);
			_insertMetricMetaDataStatement.execute();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}

	@Override
	public void pushStatsMetric(int runId, int sourceId, int metricId,
			long bucketId, long duration, RunningStats runningStats) {
		try {
			_insertStatsMetricStatement.setInt(1, runId);
			_insertStatsMetricStatement.setInt(2, sourceId);
			_insertStatsMetricStatement.setInt(3, metricId);
			_insertStatsMetricStatement.setLong(4, bucketId);
			_insertStatsMetricStatement.setLong(5, duration);
			
			_insertStatsMetricStatement.setDouble(6, runningStats.getMean());
			_insertStatsMetricStatement.setDouble(7, runningStats.getStandardDeviation());
			_insertStatsMetricStatement.setDouble(8, runningStats.getSumOfSquares());
			_insertStatsMetricStatement.setInt(9, runningStats.getCount());
			_insertStatsMetricStatement.setDouble(10, runningStats.getMin());
			_insertStatsMetricStatement.setDouble(11, runningStats.getMax());

			_insertStatsMetricStatement.execute();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}

	@Override
	public void pushCountMetric(int runId, int sourceId, int metricId,
			long bucketId, long duration, long count) {
		pushCountBasedMetric(runId, sourceId, metricId, bucketId, duration, false, count);
	}

	@Override
	public void pushThroughputMetric(int runId, int sourceId, int metricId,
			long bucketId, long duration, long count) {
		pushCountBasedMetric(runId, sourceId, metricId, bucketId, duration, true, count);
	}
	
	private void pushCountBasedMetric(int runId, int sourceId, int metricId, 
			long bucketId, long duration, boolean isThroughput, long count) {
		try {
			_insertCountMetricStatement.setInt(1, runId);
			_insertCountMetricStatement.setInt(2, sourceId);
			_insertCountMetricStatement.setInt(3, metricId);
			_insertCountMetricStatement.setLong(4, bucketId);
			_insertCountMetricStatement.setLong(5, duration);
			
			_insertCountMetricStatement.setBoolean(6, isThroughput);
			_insertCountMetricStatement.setLong(7, count);

			_insertCountMetricStatement.execute();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}
	
	@Override
	public void onEndOfBatch() {
		try {
			_connection.commit();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}

	@Override
	public boolean isClosed() {
		return _isClosed;
	}
	
}
