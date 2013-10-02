package com.adamroughton.concentus.application;

import java.nio.charset.StandardCharsets;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.data.ChunkReader;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.data.model.kryo.MatchingDataStrategy;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.UserEffectSet;
import com.esotericsoftware.minlog.Log;

public class CrowdAloud implements ApplicationVariant {
	
	public enum Mode {
		TEXT,
		SYMBOL
	}
	
	@Override
	public InstanceFactory<? extends CollectiveApplication> getApplicationFactory(long tickDuration) {
		return new ApplicationFactory(tickDuration);
	}

	@Override
	public InstanceFactory<? extends ClientAgent> getAgentFactory() {
		return new TextAgentFactory();
	}
	
	@Override
	public String name() {
		return "CrowdAloud!";
	}
	
	public enum ActionType {
		SIGNAL
		;
		public int getId() {
			return ordinal();
		}
	}
	
	public enum EffectType {
		SET_VAR_VALUE
		;
		public int getId() {
			return ordinal();
		}
	}
	
	private final static CollectiveVariableDefinition TOP_SIGNAL_VAR = 
			new CollectiveVariableDefinition(0, 25);

	public static class ApplicationFactory implements InstanceFactory<Application> {

		private long _tickDuration;
		
		// for Kryo
		@SuppressWarnings("unused")
		private ApplicationFactory() { }
		
		public ApplicationFactory(long tickDuration) {
			_tickDuration = tickDuration;
		}
		
		@Override
		public Application newInstance() {
			return new Application(_tickDuration);
		}

		@Override
		public Class<Application> instanceType() {
			return Application.class;
		}
		
	}
	
	public static class TextAgentFactory implements InstanceFactory<TextAgent> {

		@Override
		public TextAgent newInstance() {
			return new TextAgent();
		}

		@Override
		public Class<TextAgent> instanceType() {
			return TextAgent.class;
		}
		
	}
	
	public static class Application implements CollectiveApplication {

		private long _tickDuration;
		
		public Application(long tickDuration) {
			_tickDuration = tickDuration;
		}
		
		@Override
		public void processAction(UserEffectSet effectSet, int actionTypeId,
				ResizingBuffer actionData) {
			if (actionTypeId == ActionType.SIGNAL.getId()) {
				effectSet.newEffect(TOP_SIGNAL_VAR.getVariableId(), EffectType.SET_VAR_VALUE.getId(), actionData);
			} else {
				Log.warn("CrowdAloud.Application.processAction: Unknown action type ID " + actionTypeId);
			}
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			int effectTypeId = effect.getEffectTypeId(); 
			if (effectTypeId == EffectType.SET_VAR_VALUE.getId()) {
				long elapsedTime = Math.max(0, time - effect.getStartTime());
				int score;
				if (elapsedTime < 1000) {
					score = 100 - (int) (((double) elapsedTime / (double) 1000) * 10);
				} else if (elapsedTime < 5000) {
					score = 100 - (int) (((double) elapsedTime / (double) 5000) * 100);
				} else {
					score = 0;
				}
				return new CandidateValue(new MatchingDataStrategy(), effect.getVariableId(), score, effect.getData());
			} else {
				throw new RuntimeException("Unknown effect type ID " + effectTypeId);
			}
		}

		@Override
		public long getTickDuration() {
			return _tickDuration;
		}

		@Override
		public CollectiveVariableDefinition[] variableDefinitions() {
			return new CollectiveVariableDefinition[] { TOP_SIGNAL_VAR };
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> variables) {
			CollectiveVariable topSignalVar = variables.get(TOP_SIGNAL_VAR.getVariableId());
			ChunkWriter topWordWriter = new ChunkWriter(updateData);
			for (CandidateValue signalValue : topSignalVar) {
				topWordWriter.getChunkBuffer().writeInt(0, signalValue.getScore());
				topWordWriter.getChunkBuffer().writeBytes(ResizingBuffer.INT_SIZE, signalValue.getValueData());
				topWordWriter.commitChunk();
			}
			topWordWriter.finish();
		}
		
	}
	
	public static class TextAgent implements ClientAgent {

		private static String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOP!?0123456789";

		private long _clientIdBits = -1;
		private long _inputCountUntilSignal = 0;
		private long _inputCountSeq = 0;
		
		private String _currentWord; 
		
		@Override
		public void setClientId(long clientIdBits) {
			_clientIdBits = clientIdBits;
			_inputCountUntilSignal = clientIdBits % 20;
		}
		
		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			long inputSeq = _inputCountSeq++;
			
			if (_inputCountUntilSignal-- <= 0) {
				actionEvent.setActionTypeId(ActionType.SIGNAL.getId());
				if (_currentWord == null || inputSeq % 7 != 0) {
					_currentWord = generateWord();
				}
				actionEvent.getActionDataSlice().writeString(0, _currentWord, StandardCharsets.UTF_16);
				
				_inputCountUntilSignal = (inputSeq + _clientIdBits) % 15;
				return true;
			} else {
				return false;
			}
		}
		
		private String generateWord() {
			int wordLength = Math.max(1, Math.abs((int) (_clientIdBits - _inputCountSeq) % 140));
			char[] characters = new char[wordLength];
			for (int i = 0; i < wordLength; i++) {
				int charPos = (int) (_inputCountSeq * i) % CHARACTERS.length();
				characters[i] = CHARACTERS.charAt(charPos);
			}
			return new String(characters);
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
			if (_clientIdBits != -1 && ClientId.fromBits(_clientIdBits).getClientIndex() == 0) {
				ChunkReader updateChunkReader = new ChunkReader(update.getData());
				StringBuilder stateBuilder = new StringBuilder();
				stateBuilder.append("update [");
				boolean isFirst = true;
				int chunkCount = 0;
				for (ResizingBuffer chunkBuffer : updateChunkReader.asBuffers()) {
					if (isFirst) {
						isFirst = false;
					} else {
						stateBuilder.append(", ");
					}
					chunkCount++;
					int score = chunkBuffer.readInt(0);
					String word = chunkBuffer.readString(ResizingBuffer.INT_SIZE, StandardCharsets.UTF_16);
					
					stateBuilder.append(String.format("{'%s': %d}", word, score));
				}
				stateBuilder.append(chunkCount);
				stateBuilder.append("]");
				Log.info(stateBuilder.toString());
			}
			
		}
		
	}
	
}
