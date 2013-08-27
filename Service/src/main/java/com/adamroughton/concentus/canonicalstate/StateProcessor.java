/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.canonicalstate;

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.events.bufferbacked.FullCollectiveVarInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.PartialCollectiveVarInputEvent;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.model.CollectiveApplication;

public class StateProcessor<TBuffer extends ResizingBuffer> implements DeadlineBasedEventHandler<TBuffer> {
	
	private final Clock _clock;
	
	private final FullCollectiveVarInputEvent _fullCollectiveVarInputEvent = new FullCollectiveVarInputEvent();
	private final PartialCollectiveVarInputEvent _partialCollectiveVarInputEvent = new PartialCollectiveVarInputEvent();
	private final CanonicalStateUpdate _canonicalStateUpdate = new CanonicalStateUpdate();
	
	private final CollectiveApplication _application;
	private final int[] _actionProcessorIds;
	private final IncomingEventHeader _recvHeader;
	private final SendQueue<OutgoingEventHeader, TBuffer> _pubSendQueue;

	private final MetricContext _metricContext;
	private final MetricGroup _metrics;
	private final CountMetric _errorCountMetric;
	
	private long _lastTickTime = -1;
	private long _nextTickTime = -1;
	private long _simTime = 0;
	private long _accumulator = 0; // Carry over left over sim time from last update
	
	private long _updateId = 0;
	private long _nextMetricTime = -1;
	private boolean _sendMetric = false;

	public StateProcessor(
			Clock clock,
			int[] actionProcessorIds,
			CollectiveApplication application,
			IncomingEventHeader recvHeader,
			SendQueue<OutgoingEventHeader, TBuffer> pubSendQueue,
			MetricContext metricContext) {
		_clock = Objects.requireNonNull(clock);
		_application = Objects.requireNonNull(application);
		_actionProcessorIds = Objects.requireNonNull(actionProcessorIds);
		_recvHeader = Objects.requireNonNull(recvHeader);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		_metricContext = Objects.requireNonNull(metricContext);

		_metrics = new MetricGroup();
		String reference = name();
		_errorCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "errorCount", false));
	}
	
	@Override
	public void onEvent(TBuffer event, long sequence, boolean isEndOfBatch)
			throws Exception {
//		if (!EventHeader.isValid(event, 0)) {
//			_errorCountMetric.push(1);
//			return;
//		}
//		
//		
//		
//		EventPattern.readContent(event, _recvHeader, _inputEvent, new EventReader<IncomingEventHeader, ActionEvent>() {
//
//			@Override
//			public void read(IncomingEventHeader header, ActionEvent event) {
//				processInput(event);
//			}
//			
//		});
		
	}

	@Override
	public void onDeadline() {
//		if (_sendMetric) {
//			_metrics.publishPending();
//		} else {
//			long now = _clock.currentMillis();
//			long frameTime = now - _lastTickTime;
//			_accumulator += frameTime;
//			
//			long dt = Constants.TIME_STEP_IN_MS;
//			while (_accumulator >= dt) {
//				_stateLogic.tick(_simTime, dt);
//				_simTime += dt;
//				_accumulator -= dt;
//			}
//			_lastTickTime = now;
//			
//			_updateId++;
//			sendUpdateEvent(_updateId, _simTime);
//		}
	}

	@Override
	public long moveToNextDeadline(long pendingEventCount) {
		if (_lastTickTime == -1) {
			_lastTickTime = _clock.currentMillis();
			_nextTickTime = _clock.currentMillis();
		} else {
			_nextTickTime = _lastTickTime + Constants.TIME_STEP_IN_MS;
		}
		
		_nextMetricTime = _metrics.nextBucketReadyTime();
		if (_nextMetricTime < _nextTickTime) {
			_sendMetric = true;
			return _nextMetricTime;
		} else {
			_sendMetric = false;
			return _nextTickTime;
		}
	}

	@Override
	public long getDeadline() {
		if (_sendMetric) {
			return _nextMetricTime;
		} else {
			return _nextTickTime;
		}
	}
//	
//	private void processInput(ActionEvent event) {
//
//	}
//	
//	private void sendUpdateEvent(final long updateId, final long simTime) {
//		_pubSendQueue.send(PubSubPattern.asTask(_updateEvent, new EventWriter<OutgoingEventHeader, StateUpdateEvent>() {
//
//			@Override
//			public void write(OutgoingEventHeader header, StateUpdateEvent event) {
//				event.setUpdateId(updateId);
//				event.setSimTime(simTime);
//				_stateLogic.createUpdate(event.getContentSlice());
//			}
//			
//		}));
//	}
//	

	@Override
	public String name() {
		return "stateProcessor";
	}


	
}
