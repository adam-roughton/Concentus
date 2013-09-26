package com.adamroughton.concentus.application;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.canonicalstate.direct.DirectCanonicalStateService.DirectCanonicalStateServiceDeployment;
import com.adamroughton.concentus.clienthandler.ClientHandlerService.ClientHandlerServiceDeployment;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.crowdhammer.CrowdHammer;
import com.adamroughton.concentus.crowdhammer.TestBuilder;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.adamroughton.concentus.model.UserEffectSet;

public class Experiment {

	public static void main(String[] args) throws Exception {
		long[] tickDurations = new long[] { 1000, 500, 100 };
		int[] valDataRanges = new int[] { 16, 1024 * 1024 };
		
		for (long tickDuration: tickDurations) {
			for (int valDataRange : valDataRanges) {
				// single variable
				TestBuilder builder = new TestBuilder();
				builder.usingName("singleVar_tickDur_" + tickDuration + "_valDataRange_" + valDataRange);
				
				TestVariable var = new TestVariable(25, 1000, 8, valDataRange, 8);
				ExperimentApplicationFactory applicationFactory = new ExperimentApplicationFactory(tickDuration, 1, var);
				ExperimentClientAgentFactory agentFactory = new ExperimentClientAgentFactory(1, new int[] {8});
				
				builder.withApplicationFactory(applicationFactory)
					   .withAgentFactory(agentFactory);
				
				builder.withClientCounts(1000, 2000, 4000, 10000, 20000)
					   .withRunTime(2, TimeUnit.MINUTES)
					   .withService(new DirectCanonicalStateServiceDeployment(-1, -1, 2048, 2048, -1, 2048), 1)
					   .withService(new ClientHandlerServiceDeployment(-1, 2048, 2048), 2)
					   .withWorkerCount(2);
				
				CrowdHammer.runTest(builder.build());
			}
		}	
	}

	/**
	 * Simplifies the independent variables into a set that has the most impact. Though each 
	 * collective variable can have multiple effect types (each with a different duration, data size,
	 * and different candidate value output); we simplify so that each variable has one representative
	 * effect for this experiment.
	 * @author Adam Roughton
	 *
	 */
	public static class TestVariable {
		
		private int _topNCount;
		private int _effectDataSize;
		private long _effectDuration;
		private long _candidateValDataRange;
		private int _candidateValDataSize;
		
		// for Kryo
		@SuppressWarnings("unused")
		private TestVariable() { }

		public TestVariable(int topNCount, long effectDuration, int effectDataSize, 
				long candidateValDataRange, int candidateValDataSize) {
			if (candidateValDataRange > (candidateValDataSize << 8)) {
				throw new IllegalArgumentException("The specified data range (" + candidateValDataRange + ") " +
						"is greater than the maximum range for the given data size " +
						"(data size = " + candidateValDataSize + ", maximum range = " + (candidateValDataSize << 8) + ")");
			}
			_topNCount = topNCount;
			_effectDataSize = effectDataSize;
			_effectDuration = effectDuration;
			_candidateValDataRange = candidateValDataRange;
			_candidateValDataSize = candidateValDataSize;
		}
		
		public int topNCount() {
			return _topNCount;
		}
		
		public int effectDataSize() {
			return _effectDataSize;
		}
		
		public long effectDuration() {
			return _effectDuration;
		}
		
		public long candidateValDataRange() {
			return _candidateValDataRange;
		}
		
		public int candidateValDataSize() {
			return _candidateValDataSize;
		}
	}
	
	public static class ExperimentApplication implements CollectiveApplication {
		
		private final long _tickDuration;
		private final int _actionToVarRatio;
		
		private final CollectiveVariableDefinition[] _collectiveVariables;
		private final TestVariable[] _testVariables;
		private final long[] _varSeqs; // to ensure the maximum data range is provided
		
		public ExperimentApplication(long tickDuration, int actionToVarRatio, TestVariable[] testVariables) {
			_tickDuration = tickDuration;
			_actionToVarRatio = actionToVarRatio;
			_testVariables = testVariables;
			_collectiveVariables = new CollectiveVariableDefinition[_testVariables.length];
			for (int i = 0; i < _testVariables.length; i++) {
				_collectiveVariables[i] = new CollectiveVariableDefinition(i, _testVariables[i].topNCount());
			}
			_varSeqs = new long[_testVariables.length];
		}
		
		@Override
		public void processAction(UserEffectSet effectSet, int actionTypeId,
				ResizingBuffer actionData) {
			for (int actionIndex = 0; actionIndex < _actionToVarRatio; actionIndex++) {
				// map the action type ID to a test variable
				int varId = (actionTypeId + actionIndex) % _testVariables.length;
				TestVariable var = _testVariables[varId];
				
				// generate an effect for the variable
				byte[] effectData = new byte[var.effectDataSize()];
				effectSet.newEffect(varId, 0, effectData);
			}
		}

		@Override
		public CandidateValue apply(Effect effect, long time) {
			int varId = effect.getVariableId();
			TestVariable var = _testVariables[varId];
			
			int valDataSize = var.candidateValDataSize();
			byte[] candidateValData = new byte[valDataSize];
			
			// create data for the candidate value
			long seq = _varSeqs[varId];
			_varSeqs[varId] = seq + 1; 
			
			byte[] dataSrc = new byte[8];
			BytesUtil.writeLong(dataSrc, 0, seq % var.candidateValDataRange());
			
			int baseIndex = 8 - valDataSize;
			for (int i = 0; i < candidateValData.length; i++) {
				candidateValData[i] = dataSrc[baseIndex + i];
			}
			int score = (int) ((double) Math.max(0, time - effect.getStartTime()) / (double) var.effectDuration() * 100);
			
			return new CandidateValue(varId, score, candidateValData);
		}

		@Override
		public long getTickDuration() {
			return _tickDuration;
		}

		@Override
		public CollectiveVariableDefinition[] variableDefinitions() {
			return _collectiveVariables;
		}

		@Override
		public void createUpdate(ResizingBuffer updateData, long time,
				Int2ObjectMap<CollectiveVariable> variables) {
			ChunkWriter chunkWriter = new ChunkWriter(updateData);
			for (CollectiveVariable var : variables.values()) {
				for (int i = 0; i < var.getValueCount(); i++) {
					CandidateValue value = var.getValue(i);
					ResizingBuffer chunkBuffer = chunkWriter.getChunkBuffer();
					int cursor = 0;
					byte[] valData = value.getValueData();
					cursor += chunkBuffer.writeByteSegment(cursor, valData);
					chunkBuffer.writeInt(cursor, value.getScore());
					chunkWriter.commitChunk();
				}
			}
			chunkWriter.finish();
		}
		
	}
	
	public static class ExperimentApplicationFactory implements InstanceFactory<ExperimentApplication> {

		private long _tickDuration;
		private int _actionToVarRatio;
		private TestVariable[] _testVariables;
		
		// for Kryo
		@SuppressWarnings("unused")
		private ExperimentApplicationFactory() { }
		
		public ExperimentApplicationFactory(long tickDuration, int actionToVarRatio, 
				TestVariable firstCollectiveVariable,
				TestVariable...additionalCollectiveVariables) {
			int variableCount = additionalCollectiveVariables.length + 1;
			
			if (actionToVarRatio > variableCount) {
				throw new IllegalArgumentException("Not enough variables for the given action to variable ratio " +
						"(requires " + actionToVarRatio + "variables, but there is only " + variableCount + ")");
			}
			
			_testVariables = new TestVariable[variableCount];
			_testVariables[0] = firstCollectiveVariable;
			System.arraycopy(additionalCollectiveVariables, 0, _testVariables, 1, additionalCollectiveVariables.length);
			
			_tickDuration = tickDuration;
			_actionToVarRatio = actionToVarRatio;
		}
		
		@Override
		public ExperimentApplication newInstance() {
			return new ExperimentApplication(_tickDuration, _actionToVarRatio, _testVariables);
		}

		@Override
		public Class<ExperimentApplication> instanceType() {
			return ExperimentApplication.class;
		}
		
	}
	
	public static class ExperimentClientAgent implements ClientAgent {

		private final int _variableCount;
		private final int[] _actionDataSizes;
		private long _seq = 0;
		
		public ExperimentClientAgent(int variableCount, int[] actionDataSizes) {
			_variableCount = variableCount;
			_actionDataSizes = Objects.requireNonNull(actionDataSizes);
		}
		
		@Override
		public boolean onInputGeneration(ActionEvent actionEvent) {
			int actionTypeId = (int)(actionEvent.getClientIdBits() + _seq++ % _variableCount);
			byte[] data = new byte[_actionDataSizes[(int)(_seq % _actionDataSizes.length)]];
			
			actionEvent.setActionTypeId(actionTypeId);
			actionEvent.getActionDataSlice().writeBytes(0, data);
			
			return true;
		}

		@Override
		public void onUpdate(CanonicalStateUpdate update) {
		}
		
	}
	
	public static class ExperimentClientAgentFactory implements InstanceFactory<ExperimentClientAgent> {

		private int _variableCount;
		private int[] _actionDataSizes;
		
		// for Kryo
		@SuppressWarnings("unused")
		private ExperimentClientAgentFactory() { }
		
		public ExperimentClientAgentFactory(int variableCount, int[] actionDataSizes) {
			_variableCount = variableCount;
			_actionDataSizes = Objects.requireNonNull(actionDataSizes);
			if (actionDataSizes.length < 1)
				throw new IllegalArgumentException("At least one action data size must be provided");
		}
		
		@Override
		public ExperimentClientAgent newInstance() {
			return new ExperimentClientAgent(_variableCount, _actionDataSizes);
		}

		@Override
		public Class<ExperimentClientAgent> instanceType() {
			return ExperimentClientAgent.class;
		}
		
	}
	
}
