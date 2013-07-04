package com.adamroughton.concentus.crowdhammer.metriclistener;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
			_insertRunMetaDataStatement = newPreparedStatement("insert into RunMetaData values (?, ?, ?)");
			_insertSourceDataStatement = newPreparedStatement("insert into SourceData values (?, ?)");
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
	 * 		| runId (INT) | clientCount (INT) | duration (BIGINT) |
	 * 
	 * 	SourceData:
	 * 		| sourceUUID (VARCHAR(16)) | name (VARCHAR(100)) |
	 * 
	 * 	MetricMetaData:
	 * 		| runId (INT) | sourceId (VARCHAR(16)) | metricId (INT) | isCumulative (BOOLEAN) | reference (VARCHAR(100)) | name (VARCHAR(100)) |
	 * 
	 * 	CountMetric:
	 * 		| runId (INT) | sourceId (VARCHAR(16)) | metricId (INT) | bucketId (BIGINT) | bucketDuration (BIGINT) | isThroughput (BOOLEAN) | count (BIGINT) |
	 * 
	 * 	StatsMetric:
	 * 		| runId (INT) | sourceId (VARCHAR(16)) | metricId (INT) | bucketId (BIGINT) | bucketDuration (BIGINT) | mean (DOUBLE) | stddev (DOUBLE) | 
	 * 			sumSqrs (DOUBLE) | count (INT) | min (DOUBLE) | max (DOUBLE) |
	 * 
	 */
	
	private void prepareTables() throws SQLException {
		Statement prepareTablesStatement = _connection.createStatement();
		prepareTablesStatement.setQueryTimeout(30);
		prepareTablesStatement.executeUpdate("create table if not exists RunMetaData(runId INT, clientCount INT, duration BIGINT)");
		prepareTablesStatement.executeUpdate("create table if not exists SourceData(sourceId VARCHAR(16), name VARCHAR(100))");
		prepareTablesStatement.executeUpdate("create table if not exists MetricMetaData(runId INT, sourceId VARCHAR(16), metricId INT, " +
				"isCumulative BOOLEAN, reference VARCHAR(16), name VARCHAR(16))");
		prepareTablesStatement.executeUpdate("create table if not exists CountMetric (runId INT, sourceId VARCHAR(16), metricId INT, " +
				"bucketId BIGINT, bucketDuration BIGINT, isThroughput BOOLEAN, count BIGINT)");
		prepareTablesStatement.executeUpdate("create table if not exists StatsMetric (runId INT, sourceId VARCHAR(16), metricId INT, " +
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
	public void pushRunMetaData(int runId, int clientCount, long durationMillis) {
		try {
			_insertRunMetaDataStatement.setInt(1, runId);
			_insertRunMetaDataStatement.setInt(2, clientCount);
			_insertRunMetaDataStatement.setLong(3, durationMillis);
			_insertRunMetaDataStatement.execute();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}
	
	@Override
	public void pushSourceMetaData(UUID sourceId, String name) {
		try {
			_insertSourceDataStatement.setString(1, toUUIDString(sourceId));
			_insertSourceDataStatement.setString(2, name);
			_insertSourceDataStatement.execute();
		} catch (SQLException eSql) {
			throw new RuntimeException(eSql);
		}
	}

	@Override
	public void pushMetricMetaData(int runId, UUID sourceId, int metricId,
			String reference, String metricName, boolean isCumulative) {
		try {
			_insertMetricMetaDataStatement.setInt(1, runId);
			_insertMetricMetaDataStatement.setString(2, toUUIDString(sourceId));
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
	public void pushStatsMetric(int runId, UUID sourceId, int metricId,
			long bucketId, long duration, RunningStats runningStats) {
		try {
			_insertStatsMetricStatement.setInt(1, runId);
			_insertStatsMetricStatement.setString(2, toUUIDString(sourceId));
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
	public void pushCountMetric(int runId, UUID sourceId, int metricId,
			long bucketId, long duration, long count) {
		pushCountBasedMetric(runId, sourceId, metricId, bucketId, duration, false, count);
	}

	@Override
	public void pushThroughputMetric(int runId, UUID sourceId, int metricId,
			long bucketId, long duration, long count) {
		pushCountBasedMetric(runId, sourceId, metricId, bucketId, duration, true, count);
	}
	
	private void pushCountBasedMetric(int runId, UUID sourceId, int metricId, 
			long bucketId, long duration, boolean isThroughput, long count) {
		try {
			_insertCountMetricStatement.setInt(1, runId);
			_insertCountMetricStatement.setString(2, toUUIDString(sourceId));
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

	private static String toUUIDString(UUID uuid) {
		return uuid.toString().replace("-", "");
	}
	
}
