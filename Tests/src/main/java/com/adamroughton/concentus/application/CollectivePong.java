package com.adamroughton.concentus.application;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import cern.colt.Arrays;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.data.BytesUtil;
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

public class CollectivePong implements ApplicationVariant {
	
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
		return "CollectivePong";
	}

	public enum ActionType {
		PADDLE_SET
		;
		public int getId() {
			return ordinal();
		}
	}
	
	public enum EffectType {
		PADDLE_POS,
		PADDLE_SEG_ON_OFF
		;
		public int getId() {
			return ordinal();
		}
	}
	
	private static final CollectiveVariableDefinition[] COLLECTIVE_VARIABLES;
	private static final int Y_POS_VAR_ID = 0;
	private static final int Y_POS_CHUNK_COUNT = 1024;
	private static final int Y_POS_CHUNKS_BASE_VAR_ID = 1;
	private static final int PADDLE_HEIGHT = 8;
	static {
		COLLECTIVE_VARIABLES = new CollectiveVariableDefinition[Y_POS_CHUNK_COUNT + 1];
		/**
		 * The first variable represents the Y position chosen by a participant. This
		 * variable is not used in the aggregation directly, but is used for updating neighbour
		 * or local views. The Y_POS_CHUNKS variables are used for the crowd paddle structure,
		 * with each chunk representing one unit of paddle height. This allows us to support
		 * a fuzzy paddle, where users don't have to align directly to have some part of the
		 * paddle overlap. 
		 */
		COLLECTIVE_VARIABLES[Y_POS_VAR_ID] = new CollectiveVariableDefinition(0, 0);
		for (int i = 0; i < Y_POS_CHUNK_COUNT; i++) {
			COLLECTIVE_VARIABLES[Y_POS_CHUNKS_BASE_VAR_ID + i] = new CollectiveVariableDefinition(Y_POS_CHUNKS_BASE_VAR_ID + i, 2);
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

		@Override
		public Agent newInstance() {
			return new Agent();
		}

		@Override
		public Class<Agent> instanceType() {
			return Agent.class;
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
			if (actionTypeId == ActionType.PADDLE_SET.getId()) {
				// get paddle top position
				int paddleTopPos = actionData.readInt(0);
				
				if (paddleTopPos > Y_POS_CHUNK_COUNT - PADDLE_HEIGHT)
					throw new IllegalStateException("The paddle position must fit " +
							"in the bounds of the game (0 - " + Y_POS_CHUNK_COUNT + "); " +
							"was [" + paddleTopPos + " - " + paddleTopPos + PADDLE_HEIGHT + "]");
				
				for (int i = 0; i < Y_POS_CHUNK_COUNT; i++) {
					byte[] onOffFlag = new byte[1];
					if (i >= paddleTopPos && i < paddleTopPos + PADDLE_HEIGHT) {	
						BytesUtil.writeBoolean(onOffFlag, 0, true);
					} else {
						BytesUtil.writeBoolean(onOffFlag, 0, false);
					}
					effectSet.newEffect(Y_POS_CHUNKS_BASE_VAR_ID + i, EffectType.PADDLE_SEG_ON_OFF.getId(), onOffFlag, false);
				}
				
				effectSet.newEffect(Y_POS_VAR_ID, EffectType.PADDLE_POS.getId(), actionData.readBytes(0, ResizingBuffer.INT_SIZE));
			}
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			int effectTypeId = effect.getEffectTypeId(); 
			if (effectTypeId == EffectType.PADDLE_SEG_ON_OFF.getId() && !effect.isCancelled()) {
				return new CandidateValue(effect.getVariableId(), 1, effect.getData());
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
			return COLLECTIVE_VARIABLES;
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> variables) {
			// in the real game we would compute the pong ball position and
			// perform collision detection: we omit these here to instead
			// focus on the collective effect
			
			int[] paddleBar = new int[Y_POS_CHUNK_COUNT];
			for (int i = 0; i < Y_POS_CHUNK_COUNT; i++) {
				int posScore = 0;
				if (variables.containsKey(Y_POS_CHUNKS_BASE_VAR_ID + i)) {
					int onCount = 0;
					int totalCount = 0;
					for(CandidateValue value : variables.get(Y_POS_CHUNKS_BASE_VAR_ID + i)) {
						if (BytesUtil.readBoolean(value.getValueData(), 0)) {
							onCount = value.getScore();
						}
						totalCount += value.getScore();
					}
					posScore = (int) (((double) onCount / (double) totalCount) * 100);
				}
				paddleBar[i] = posScore;
			}

			// do game logic with paddleBar here
			
			// write out update
			updateData.writeInt(0, paddleBar.length);
			int cursor = 0;
			for (int score : paddleBar) {
				cursor += ResizingBuffer.INT_SIZE;
				updateData.writeInt(cursor, score);
			}
		}
		
	}
	
	public static class Agent implements ClientAgent {

		private long _clientIdBits = -1;
		private long _inputCountSeq = 0;
		
		private int _prevPaddlePos = 512;
		
		@Override
		public void setClientId(long clientIdBits) {
			_clientIdBits = clientIdBits;
		}
		
		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			long inputSeq = _inputCountSeq++;
			actionEvent.setActionTypeId(ActionType.PADDLE_SET.getId());
			int paddlePos = (int) ((_prevPaddlePos + ((inputSeq % 2 == 0)? -1 : 1) * (_clientIdBits % 50)) % 1024);
			actionEvent.getActionDataSlice().writeInt(0, paddlePos);
			_prevPaddlePos = paddlePos;
			return true;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
			if (_clientIdBits != -1 && ClientId.fromBits(_clientIdBits).getClientIndex() == 0) {
				ResizingBuffer updateBuffer = update.getBuffer();
				int paddleBarLength = updateBuffer.readInt(0);
				if (paddleBarLength > 1024) throw new RuntimeException("Paddle bar size was " + paddleBarLength);
				int cursor = 0;
				int[] paddleBar = new int[paddleBarLength];
				for (int i = 0; i < paddleBarLength; i++) {
					cursor += ResizingBuffer.INT_SIZE;
					paddleBar[i] = updateBuffer.readInt(cursor);
				}
				Log.info(Arrays.toString(paddleBar));
			}
			
		}
		
	}
	
}
