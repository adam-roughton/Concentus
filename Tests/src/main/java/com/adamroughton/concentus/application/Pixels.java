package com.adamroughton.concentus.application;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.data.model.kryo.MatchingDataStrategy;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.UserEffectSet;
import com.esotericsoftware.minlog.Log;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public class Pixels implements ApplicationVariant {
	
	@Override
	public InstanceFactory<? extends CollectiveApplication> getApplicationFactory(long tickDuration) {
		return new ApplicationFactory(tickDuration);
	}

	@Override
	public InstanceFactory<? extends ClientAgent> getAgentFactory() {
		return new AgentFactory();
	}
	
	@Override
	public String name() {
		return "Pixels";
	}
	
	public enum ActionType {
		SET_PIXEL_COLOUR
		;
		public int getId() {
			return ordinal();
		}
	}
	
	public enum EffectType {
		PIXEL_COLOUR
		;
		public int getId() {
			return ordinal();
		}
	}
	
	/**
	 * Canvas of 1024 x 1024 pixels (1MP)
	 */
	private final static CollectiveVariableDefinition[] PIXEL_VARIABLES;
	static {
		PIXEL_VARIABLES = new CollectiveVariableDefinition[1024 * 1024];
		for (int i = 0; i < PIXEL_VARIABLES.length; i++) {
			PIXEL_VARIABLES[i] = new CollectiveVariableDefinition(i, 1);
		}
	}

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
	
	public static class AgentFactory implements InstanceFactory<Agent> {

		private final boolean _logUpdates = ApplicationVariant.SharedConfig.logUpdatesOneClientPerWorker;
		
		@Override
		public Agent newInstance() {
			return new Agent(_logUpdates);
		}

		@Override
		public Class<Agent> instanceType() {
			return Agent.class;
		}
		
	}
	
	public static class Application implements CollectiveApplication {

		private long _tickDuration;
		
		/*
		 * lazily instantiate on createUpdate as we only need
		 * the canonical state processor to have a copy
		 */
		private byte[] _currentFrame = null;
		
		public Application(long tickDuration) {
			_tickDuration = tickDuration;
		}
		
		@Override
		public void processAction(UserEffectSet effectSet, int actionTypeId,
				ResizingBuffer actionData) {
			if (actionTypeId == ActionType.SET_PIXEL_COLOUR.getId()) {
				int pixelId = actionData.readInt(0);
				byte[] colourData = actionData.readBytes(INT_SIZE, FLOAT_SIZE);
				effectSet.newEffect(pixelId, EffectType.PIXEL_COLOUR.getId(), colourData);
			}
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			int effectTypeId = effect.getEffectTypeId(); 
			if (effectTypeId == EffectType.PIXEL_COLOUR.getId()) {
				long elapsedTime = Math.max(0, time - effect.getStartTime());
				int score;
				if (elapsedTime < 5000) {
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
			return PIXEL_VARIABLES;
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> variables) {
			// low latency video compression will be needed to transmit the
			// data to clients (8MB uncompressed); alternatively we could limit
			// the colour space (e.g. 4bits per colour for 16 colours)
			if (_currentFrame == null) {
				// (HSL), we have saturation always = 1
				_currentFrame = new byte[PIXEL_VARIABLES.length * 2 * FLOAT_SIZE];
			}
			
			int cursor = 0;
			for (int pixelId = 0; pixelId < PIXEL_VARIABLES.length; pixelId++) {
				if (variables.containsKey(pixelId)) {
					int topScore = 0;
					int totalCount = 0;
					CollectiveVariable pixelVariable = variables.get(pixelId);
					for (int vRank = 0; vRank < pixelVariable.getValueCount(); vRank++) {
						CandidateValue candidatePixelValue = pixelVariable.getValue(vRank);
						if (vRank == 0) {
							topScore = candidatePixelValue.getScore();
							System.arraycopy(candidatePixelValue.getValueData(), 0, _currentFrame, cursor, FLOAT_SIZE);
							cursor += FLOAT_SIZE;
						}
						totalCount += candidatePixelValue.getScore();
					}
					float lightness = (((float) topScore / (float) totalCount) * 0.5f) + 0.5f;
					BytesUtil.writeFloat(_currentFrame, cursor, lightness);
					cursor += FLOAT_SIZE;
				} else {
					// write a white pixel if no value from the crowd
					for (int i = 0; i < 2; i++) {
						BytesUtil.writeFloat(_currentFrame, cursor, 1);
						cursor += ResizingBuffer.FLOAT_SIZE;
					}
				}
			}
			// for now assume some wonderful compression (or other technique for representing
			// the data in a terse way)			
			updateData.writeByte(256 * 1024 - 1, (byte) 1);
		}
		
	}
	
	public static class Agent implements ClientAgent {

		
		// used for hue generation
		private static final int HUE_RANGE = 1024 * 1024;
		
		private final boolean _logUpdates;
		private long _clientIdBits = -1;
		private long _inputCountUntilSignal = 0;
		private long _inputCountSeq = 0;
		
		private float _currentHue = -1;
		private int _pixelId = 0;
		
		@SuppressWarnings("unused")
		private Agent() {
			_logUpdates = false;
		}
		
		public Agent(boolean logUpdates) {
			_logUpdates = logUpdates;
		}
		
		@Override
		public void setClientId(long clientIdBits) {
			_clientIdBits = clientIdBits;
			_pixelId = (int) _clientIdBits % (1024 * 1024);
		}
		
		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			long inputSeq = _inputCountSeq++;
			
			if (_inputCountUntilSignal-- <= 0) {
				actionEvent.setActionTypeId(ActionType.SET_PIXEL_COLOUR.getId());
				if (_currentHue == -1 || inputSeq % 7 != 0) {
					_currentHue = generateHue();
				}
				ResizingBuffer actionDataSlice = actionEvent.getActionDataSlice();
				actionDataSlice.writeInt(0, _pixelId);
				actionDataSlice.writeFloat(INT_SIZE, _currentHue);
				_inputCountUntilSignal = (inputSeq + _clientIdBits) % 15;
				return true;
			} else {
				return false;
			}
		}
		
		private float generateHue() {
			return ((float) (_clientIdBits - _inputCountSeq) % HUE_RANGE) / (float) HUE_RANGE;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
			if (_logUpdates) {
				Log.info("Got update of size " + update.getData().getContentSize() + " for time " + update.getTime());
			}
		}
		
	}
	
}
