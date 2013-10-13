package com.adamroughton.concentus.crowdhammer.metriclistener;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Test;

import com.adamroughton.concentus.crowdhammer.ClientAgent;
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

public class TestSqliteMetricStore extends TestSqliteMetricStoreBase {
	
	@Test
	public void testPushRunMetaData() {
		final Set<Pair<String, Integer>> deploymentInfo = new HashSet<>();
		deploymentInfo.add(new Pair<>("worker", 5));
		deploymentInfo.add(new Pair<>("clientHandler", 10));
		deploymentInfo.add(new Pair<>("canonicalState", 1));
		final String deploymentName = "deployment0";
		
		_metricStore.pushTestRunMetaData(0, "testRun0", 1000, 2000, TestApplicationClass.class, deploymentName, TestAgentClass.class, deploymentInfo);
		_metricStore.onEndOfBatch();
		assertDbState("select * from RunMetaData", new AssertDelegate() {

			@Override
			public void assertDbState(ResultSet resultSet) throws SQLException {
				assertTrue(resultSet.next());
				assertEquals(0, resultSet.getInt("runId"));
				assertEquals(1000, resultSet.getInt("clientCount"));
				assertEquals(2000, resultSet.getLong("duration"));
				assertEquals(TestApplicationClass.class.getName(), resultSet.getString("applicationClass"));
				assertEquals(deploymentName, resultSet.getString("deploymentName"));
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
			return false;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
		}

		@Override
		public void setClientId(long clientIdBits) {
		}
		
	}
}
