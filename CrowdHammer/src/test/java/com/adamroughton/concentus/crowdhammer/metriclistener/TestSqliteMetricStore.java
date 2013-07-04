package com.adamroughton.concentus.crowdhammer.metriclistener;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSqliteMetricStore {

	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(String.format("The driver for handling the sqlite database " +
					"for metric results was not found: '%s'", e.getMessage()), e);
		}
	}
	
	private static Path _tempDir;
	private Path _dbFile;
	private SqliteMetricStore _metricStore;
	
	@BeforeClass
	public static void setUpClass() throws IOException {
		_tempDir = Files.createTempDirectory("metricDbs");
	}
	
	@AfterClass
	public static void tearDownClass() throws IOException {
		Files.walkFileTree(_tempDir, new FileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path dir,
					BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file,
					BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc)
					throws IOException {
				return FileVisitResult.TERMINATE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc)
					throws IOException {
				return FileVisitResult.CONTINUE;
			}
		});
		Files.delete(_tempDir);
	}
	
	@Before
	public void setUp() throws IOException {
		_dbFile = Files.createTempFile(_tempDir, "testMetric", "sqlite");
		_metricStore = new SqliteMetricStore(_dbFile);
	}
	
	@After
	public void tearDown() throws IOException {
		Files.delete(_dbFile);
	}
	
	@Test
	public void testPushRunMetaData() {
		_metricStore.pushRunMetaData(0, 1000, 2000);
		_metricStore.onEndOfBatch();
		assertDbState("select * from RunMetaData", new AssertDelegate() {

			@Override
			public void assertDbState(ResultSet resultSet) throws SQLException {
				assertTrue(resultSet.next());
				assertEquals(0, resultSet.getInt("runId"));
				assertEquals(1000, resultSet.getInt("clientCount"));
				assertEquals(2000, resultSet.getLong("duration"));
				assertFalse(resultSet.next());
			}
		});
		
	}
	
	private void assertDbState(String sql, AssertDelegate resultSetEvalDelegate) {
		try (Connection connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", _dbFile.toString()))) {
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);
			ResultSet resultSet = statement.executeQuery(sql);
			resultSetEvalDelegate.assertDbState(resultSet);
		} catch (SQLException eSql) {
			throw new RuntimeException("Error asserting db state", eSql);
		}
	}
	
	private static interface AssertDelegate {
		void assertDbState(ResultSet resultSet) throws SQLException;
	}
	
}
