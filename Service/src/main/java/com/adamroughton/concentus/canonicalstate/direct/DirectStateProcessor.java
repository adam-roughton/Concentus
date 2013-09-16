package com.adamroughton.concentus.canonicalstate.direct;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Collections;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.canonicalstate.CanonicalStateProcessor;
import com.adamroughton.concentus.canonicalstate.TickTimer.TickStrategy;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.model.CollectiveVariableDefinition;
import com.lmax.disruptor.LifecycleAware;

public class DirectStateProcessor<TBuffer extends ResizingBuffer> implements DeadlineBasedEventHandler<ComputeStateEvent>, LifecycleAware {

	private final CollectiveVariableDefinition[] _variableDefinitions;
	private final CanonicalStateProcessor<TBuffer> _canonicalStateProcessor;
	private final Int2ObjectMap<CollectiveVariable> _variablesMap;
	private final MetricGroup _metricGroup;
	
	public DirectStateProcessor(
			CollectiveApplication application,
			Clock clock,
			SendQueue<OutgoingEventHeader, TBuffer> canonicalStatePubQueue,
			TickStrategy tickStrategy,
			MetricContext metricContext) {
		_metricGroup = new MetricGroup();
		_canonicalStateProcessor = new CanonicalStateProcessor<>(application, clock, canonicalStatePubQueue, tickStrategy, _metricGroup, metricContext);				
		_variablesMap = new Int2ObjectOpenHashMap<>();
		_variableDefinitions = application.variableDefinitions();
	}
	
	@Override
	public void onStart() {
		_canonicalStateProcessor.start();
	}

	@Override
	public void onShutdown() {
		_canonicalStateProcessor.stop();
	}
	
	@Override
	public void onEvent(ComputeStateEvent event, long sequence,
			boolean endOfBatch) throws Exception {
		// sort the values
		Collections.sort(event.candidateValues);
		
		long time = event.time;
		prepareVariables(_variablesMap);
		
		// create the collective variable set by processing in sorted order
		CollectiveVariable currentVariable = null;
		CandidateValue currentValue = null;
		for (CandidateValue value : event.candidateValues) {
			if (currentVariable == null) {
				currentValue = value;
				currentVariable = _variablesMap.get(value.getVariableId());
			} else if (currentValue.getVariableId() == value.getVariableId()) {
				if (currentValue.matchesValue(value)) {
					currentValue = currentValue.union(value);
				} else {
					currentVariable.push(currentValue);
					currentValue = value;
				}
			} else {
				currentVariable.push(currentValue);
				currentVariable = _variablesMap.get(value.getVariableId());
				currentValue = value;
			}
		}
		if (currentValue != null) {
			currentVariable.push(currentValue);
		}
		_canonicalStateProcessor.onTickCompleted(time, _variablesMap);
	}
	
	private void prepareVariables(Int2ObjectMap<CollectiveVariable> variableMap) {
		variableMap.clear();
		for (CollectiveVariableDefinition varDef : _variableDefinitions) {
			variableMap.put(varDef.getVariableId(), new CollectiveVariable(varDef.getTopNCount(), varDef.getVariableId()));
		}
	}

	@Override
	public void onDeadline() {
		_metricGroup.publishPending();
	}

	@Override
	public long moveToNextDeadline(long pendingCount) {
		return _metricGroup.nextBucketReadyTime();
	}

	@Override
	public long getDeadline() {
		return _metricGroup.nextBucketReadyTime();
	}

	@Override
	public String name() {
		return "Direct Canonical State Processor";
	}

}
