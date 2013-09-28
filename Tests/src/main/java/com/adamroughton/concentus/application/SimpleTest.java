package com.adamroughton.concentus.application;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.ListClientCount;
import com.adamroughton.concentus.crowdhammer.Test;
import com.adamroughton.concentus.crowdhammer.TestDeploymentSet;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ChunkReader;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.UserEffectSet;
import com.esotericsoftware.minlog.Log;

public class SimpleTest {

	public static void main(String[] args) throws Exception {
		SimpleApplicationFactory applicationFactory = new SimpleApplicationFactory();
		SimpleClientAgentFactory agentFactory = new SimpleClientAgentFactory();
		ListClientCount clientCountIterable = new ListClientCount(1000, 2000, 3000, 4000, 5000, 10000, 20000, 40000, 80000);
		DeploymentConfigurator[] canonicalStateDepConditions = new DeploymentConfigurator[] { new SingleDisruptorConfigurator(), new SparkDriverConfigurator() };
		
		Test test;
		for (DeploymentConfigurator configurator : canonicalStateDepConditions) {
			for (int i = 0; i < 5; i++) {
				TestDeploymentSet deploymentSet = new TestDeploymentSet(configurator.deploymentName(), agentFactory);
					configurator.configure(deploymentSet, 1)
					.addDeployment(new ClientHandlerServiceDeployment(-1, 2048, 2048), 1)
					.setWorkerCount(1);
				test = new Test("simple", applicationFactory, deploymentSet, clientCountIterable, 2, TimeUnit.MINUTES);
				CrowdHammer.runTest(test);
			}
		}
	}
	
	public static class SimpleApplicationFactory implements InstanceFactory<SimpleApplication> {

		@Override
		public SimpleApplication newInstance() {
			return new SimpleApplication();
		}

		@Override
		public Class<SimpleApplication> instanceType() {
			return SimpleApplication.class;
		}
		
	}
	
	public static class SimpleApplication implements CollectiveApplication {

		private static final String[] PHRASES = new String[] { "Horay!", "Awesome", "Hmmmmm", "What?!", "That really sucks" };
		
		private long _seq = 0;
		
		@Override
		public void processAction(UserEffectSet effectSet, int actionTypeId,
				ResizingBuffer actionData) {
			if (actionTypeId == 0) {
				byte[] data = PHRASES[(int)((_seq++ / 1000) % PHRASES.length)].getBytes();
				effectSet.newEffect(0, 0, data);
			}		
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			long timeActive = time - effect.getStartTime();
			int score;
			if (timeActive < 10000) 
				score = 100;
			else if (timeActive < 20000) 
				score = (int) Math.ceil(100 * (timeActive - 10000) / 900);
			else
			 	score = 0;
			return new CandidateValue(effect.getVariableId(), score, effect.getData());
		}

		@Override
		public long getTickDuration() {
			return 1000;
		}

		@Override
		public CollectiveVariableDefinition[] variableDefinitions() {
			return new CollectiveVariableDefinition[] {
					new CollectiveVariableDefinition(0, 50)
			};
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> collectiveVariableSet) {
			ChunkWriter chunkWriter = new ChunkWriter(updateData);
			ResizingBuffer chunkBuffer = chunkWriter.getChunkBuffer();
			
			CollectiveVariable topN = collectiveVariableSet.get(0);
			for (int i = 0; i < topN.getValueCount(); i++) {
				CandidateValue val = topN.getValue(i);
				chunkBuffer.writeInt(0, val.getScore());
				chunkBuffer.writeBytes(ResizingBuffer.INT_SIZE, val.getValueData());
				chunkWriter.commitChunk();
			}
			chunkWriter.finish();
		}
		
	}
	
	public static class SimpleClientAgentFactory implements InstanceFactory<SimpleClientAgent> {

		@Override
		public SimpleClientAgent newInstance() {
			return new SimpleClientAgent();
		}

		@Override
		public Class<SimpleClientAgent> instanceType() {
			return SimpleClientAgent.class;
		}
		
	}
	
	public static class SimpleClientAgent implements ClientAgent {

		private long _clientIdBits = -1;
		
		@Override
		public void setClientId(long clientIdBits) {
			_clientIdBits = clientIdBits;
		}
		
		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			actionEvent.setActionTypeId(0);
			return true;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
			if (_clientIdBits != -1 && ClientId.fromBits(_clientIdBits).getClientIndex() == 0) {
				ChunkReader updateChunkReader = new ChunkReader(update.getData());
				StringBuilder stateBuilder = new StringBuilder();
				stateBuilder.append("update [");
				boolean isFirst = true;
				for (byte[] chunk : updateChunkReader) {
					if (isFirst) {
						isFirst = false;
					} else {
						stateBuilder.append(", ");
					}
					
					ArrayBackedResizingBuffer chunkBuffer = new ArrayBackedResizingBuffer(chunk);
					int score = chunkBuffer.readInt(0);
					String data = new String(chunkBuffer.readBytes(ResizingBuffer.INT_SIZE, chunk.length - ResizingBuffer.INT_SIZE));
					stateBuilder.append(String.format("{'%s': %d}", data, score));
				}
				stateBuilder.append("]");
				Log.info(stateBuilder.toString());
			}
			
		}


		
	}
	
}
