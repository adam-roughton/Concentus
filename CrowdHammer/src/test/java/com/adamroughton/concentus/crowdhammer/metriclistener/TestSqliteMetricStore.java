package com.adamroughton.concentus.crowdhammer.metriclistener;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

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
import java.util.HashSet;
import java.util.Set;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.crowdhammer.metriccollector.SqliteMetricStore;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.UserEffectSet;

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
		final Set<Pair<String, Integer>> deploymentInfo = new HashSet<>();
		deploymentInfo.add(new Pair<>("worker", 5));
		deploymentInfo.add(new Pair<>("clientHandler", 10));
		deploymentInfo.add(new Pair<>("canonicalState", 1));
		
		_metricStore.pushTestRunMetaData(0, "testRun0", 1000, 2000, TestApplicationClass.class, TestAgentClass.class, deploymentInfo);
		_metricStore.onEndOfBatch();
		assertDbState("select * from RunMetaData", new AssertDelegate() {

			@Override
			public void assertDbState(ResultSet resultSet) throws SQLException {
				assertTrue(resultSet.next());
				assertEquals(0, resultSet.getInt("runId"));
				assertEquals(1000, resultSet.getInt("clientCount"));
				assertEquals(2000, resultSet.getLong("duration"));
				assertEquals(TestApplicationClass.class.getName(), resultSet.getString("applicationClass"));
				assertEquals(TestAgentClass.class.getName(), resultSet.getString("agentClass"));
				assertFalse(resultSet.next());
			}
		});
		
		assertDbState("select * from RunDeploymentData", new AssertDelegate() {
			
			@Override
			public void assertDbState(ResultSet resultSet) throws SQLException {
				Set<Triplet<Integer, String, Integer>> expected = new HashSet<>();
				for (Pair<String, Integer> deployment : deploymentInfo) {
					expected.add(new Triplet<>(0, deployment.getValue0(), deployment.getValue1()));
				}
				while (resultSet.next()) {
					int runId = resultSet.getInt("runId");
					String serviceType = resultSet.getString("serviceType");
					int count = resultSet.getInt("count");
					
					Triplet<Integer, String, Integer> actual = new Triplet<>(runId, serviceType, count);
					expected.remove(actual);
				}
				assertTrue(expected.isEmpty());
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
	
	private static class TestApplicationClass implements CollectiveApplication {

		@Override
		public void processAction(UserEffectSet effectSet, int actionTypeId,
				ResizingBuffer actionData) {
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			return null;
		}

		@Override
		public long getTickDuration() {
			return 0;
		}

		@Override
		public CollectiveVariableDefinition[] variableDefinitions() {
			return null;
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> variables) {
		}
		
	}
	
	private static class TestAgentClass implements ClientAgent {

		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
