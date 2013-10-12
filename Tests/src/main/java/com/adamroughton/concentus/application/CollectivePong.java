package com.adamroughton.concentus.application;

import java.util.Arrays;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

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
import com.adamroughton.concentus.data.model.kryo.DataAggregateStrategy;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.UserEffectSet;
import com.esotericsoftware.minlog.Log;
import static com.adamroughton.concentus.data.ResizingBuffer.*;

public class CollectivePong implements ApplicationVariant {
	
	private final int _paddleChunkCount;
	
	public CollectivePong(int paddleChunkCount) {
		_paddleChunkCount = paddleChunkCount;
	}
	
	@Override
	public InstanceFactory<? extends CollectiveApplication> getApplicationFactory(long tickDuration) {
		return new ApplicationFactory(_paddleChunkCount, tickDuration);
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
		;
		public int getId() {
			return ordinal();
		}
	}
	
	private static final int PADDLE_VAR_ID = 0;
	private static final int PADDLE_HEIGHT = 8;
	private static final CollectiveVariableDefinition PADDLE_BAR_VARIABLE = new CollectiveVariableDefinition(PADDLE_VAR_ID, 1);
	
	public static class ApplicationFactory implements InstanceFactory<Application> {

		private long _tickDuration;
		private int _paddleChunkCount;
		
		// for Kryo
		@SuppressWarnings("unused")
		private ApplicationFactory() { }
		
		public ApplicationFactory(int paddleChunkCount, long tickDuration) {
			_paddleChunkCount = paddleChunkCount;
			_tickDuration = tickDuration;
		}
		
		@Override
		public Application newInstance() {
			return new Application(_paddleChunkCount, _tickDuration);
		}

		@Override
		public Class<Application> instanceType() {
			return Application.class;
		}
		
	}
	
	public static class AgentFactory implements InstanceFactory<Agent> {

		private final boolean _logUpdates = ApplicationVariant.SharedConfig.logUpdatesOneClientPerWorker;
		private int _paddleChunkCount;
		
		private AgentFactory() { }
		
		public AgentFactory(int paddleChunkCount) {
			_paddleChunkCount = paddleChunkCount;
		}
		
		@Override
		public Agent newInstance() {
			return new Agent(_paddleChunkCount, _logUpdates);
		}

		@Override
		public Class<Agent> instanceType() {
			return Agent.class;
		}
		
	}
	
	public static class PaddleAggregateStrategy extends DataAggregateStrategy {
		
		private static final long serialVersionUID = 1L;
		
		private int _paddleChunkCount;
		
		private PaddleAggregateStrategy() { }
		
		public PaddleAggregateStrategy(int paddleChunkCount) {
			_paddleChunkCount = paddleChunkCount;
		}
		
		@Override
		protected byte[] aggregate(CandidateValue val1, CandidateValue val2) {
			byte[] sumPaddleBar = new byte[_paddleChunkCount * INT_SIZE];
			for (int i = 0; i < _paddleChunkCount; i++) {
				int offset = i * INT_SIZE;
				int paddleChunkValue = BytesUtil.readInt(val1.getValueData(), offset) + 
						BytesUtil.readInt(val2.getValueData(), offset);
				BytesUtil.writeInt(sumPaddleBar, offset, paddleChunkValue);
			}
			return sumPaddleBar;
		}
		
	}
	
	public static class Application implements CollectiveApplication {

		private final int _paddleChunkCount;
		private final long _tickDuration;
		
		public Application(int paddleChunkCount, long tickDuration) {
			_paddleChunkCount = paddleChunkCount;
			_tickDuration = tickDuration;
		}
		
		@Override
		public void processAction(UserEffectSet effectSet, int actionTypeId,
				ResizingBuffer actionData) {
			if (actionTypeId == ActionType.PADDLE_SET.getId()) {
				// get paddle top position
				int paddleTopPos = actionData.readInt(0);
				
				if (paddleTopPos > _paddleChunkCount - PADDLE_HEIGHT)
					throw new IllegalStateException("The paddle position must fit " +
							"in the bounds of the game (0 - " + _paddleChunkCount + "); " +
							"was [" + paddleTopPos + " - " + paddleTopPos + PADDLE_HEIGHT + "]");
				
				effectSet.newEffect(PADDLE_VAR_ID, EffectType.PADDLE_POS.getId(), actionData.readBytes(0, INT_SIZE));
			}
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			int effectTypeId = effect.getEffectTypeId(); 
			if (effectTypeId == EffectType.PADDLE_POS.getId()) {
				// get paddle top position
				int paddleTopPos = BytesUtil.readInt(effect.getData(), 0);
				byte[] paddleBar = new byte[_paddleChunkCount * INT_SIZE];
				
				for (int i = 0; i < PADDLE_HEIGHT; i++) {
					BytesUtil.writeInt(paddleBar, (i + paddleTopPos) * INT_SIZE, 1);
				}
				return new CandidateValue(new PaddleAggregateStrategy(), effect.getVariableId(), 1, paddleBar);
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
			return new CollectiveVariableDefinition[] { PADDLE_BAR_VARIABLE };
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> variables) {
			// in the real game we would compute the pong ball position and
			// perform collision detection: we omit these here to instead
			// focus on the collective effect
			
			byte[] paddleBar = null;
			if (variables.containsKey(PADDLE_VAR_ID)) {
				CollectiveVariable paddleVariable = variables.get(PADDLE_VAR_ID);
				if (paddleVariable.getValueCount() > 0) {
					CandidateValue paddleValue = paddleVariable.getValue(0);
					paddleBar = paddleValue.getValueData();					
				}
			}
			if (paddleBar == null) {
				paddleBar = new byte[_paddleChunkCount * INT_SIZE];
			}
			
			// do game logic here
			
			updateData.writeBytes(0, paddleBar);
		}
		
	}
	
	public static class Agent implements ClientAgent {
		
		private final boolean _logUpdates;
		private final int _paddleChunkCount;
		private long _clientIdBits = -1;
		private long _inputCountSeq = 0;
		
		private int _prevPaddlePos = 512;
		
		@SuppressWarnings("unused")
		private Agent() {
			_paddleChunkCount = 1;
			_logUpdates = false;
		}
		
		public Agent(int paddleChunkCount, boolean logUpdates) {
			_paddleChunkCount = paddleChunkCount;
			_logUpdates = logUpdates;
		}
		
		@Override
		public void setClientId(long clientIdBits) {
			_clientIdBits = clientIdBits;
		}
		
		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			long inputSeq = _inputCountSeq++;
			actionEvent.setActionTypeId(ActionType.PADDLE_SET.getId());
			
			int paddlePosChange = (int) (((inputSeq % 2 == 0)? -1 : 1) * (_clientIdBits % 50));
			int paddlePos = Math.max(0, Math.min(_prevPaddlePos + paddlePosChange, 1024));
			
			actionEvent.getActionDataSlice().writeInt(0, paddlePos);
			_prevPaddlePos = paddlePos;
			return true;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
			if (_logUpdates) {
				if (_clientIdBits != -1 && ClientId.fromBits(_clientIdBits).getClientIndex() == 0) {
					ResizingBuffer updateBuffer = update.getData();
					int[] paddleBar = new int[_paddleChunkCount];
					for (int i = 0; i < _paddleChunkCount; i++) {
						paddleBar[i] = updateBuffer.readInt(i * INT_SIZE);
					}
					Log.info(Arrays.toString(paddleBar));
				}
			}
		}
		
	}
	
}
