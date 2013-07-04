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

import java.nio.ByteBuffer;
import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientHandlerEntry;
import com.adamroughton.concentus.messaging.events.StateInputEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateInfoEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;

public class StateProcessor implements DeadlineBasedEventHandler<byte[]> {
	
	private final Clock _clock;
	
	private final StateInputEvent _inputEvent = new StateInputEvent();
	private final StateUpdateEvent _updateEvent = new StateUpdateEvent();
	private final StateUpdateInfoEvent _updateInfoEvent = new StateUpdateInfoEvent();
	
	private final StateLogic _stateLogic;
	private final IncomingEventHeader _subHeader;
	private final SendQueue<OutgoingEventHeader> _pubSendQueue;

	private final Int2LongMap _clientHandlerEventTracker;
	
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
			StateLogic stateLogic,
			IncomingEventHeader subHeader,
			SendQueue<OutgoingEventHeader> pubSendQueue,
			MetricContext metricContext) {
		_clock = Objects.requireNonNull(clock);
		_stateLogic = Objects.requireNonNull(stateLogic);
		_subHeader = Objects.requireNonNull(subHeader);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		_metricContext = Objects.requireNonNull(metricContext);

		_clientHandlerEventTracker = new Int2LongOpenHashMap(50);
		
		_metrics = new MetricGroup();
		String reference = name();
		_errorCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "errorCount", false));
	}
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean isEndOfBatch)
			throws Exception {
		boolean wasValid = _subHeader.isValid(event);
		if (wasValid) {
			EventPattern.readContent(event, _subHeader, _inputEvent, new EventReader<IncomingEventHeader, StateInputEvent>() {

				@Override
				public void read(IncomingEventHeader header, StateInputEvent event) {
					processInput(event);
				}
				
			});
		}
		if (!wasValid) _errorCountMetric.push(1);
	}

	@Override
	public void onDeadline() {
		if (_sendMetric) {
			_metrics.publishPending();
		} else {
			long now = _clock.currentMillis();
			long frameTime = now - _lastTickTime;
			_accumulator += frameTime;
			
			long dt = Constants.TIME_STEP_IN_MS;
			while (_accumulator >= dt) {
				_stateLogic.tick(_simTime, dt);
				_simTime += dt;
				_accumulator -= dt;
			}
			_lastTickTime = now;
			
			_updateId++;
			sendUpdateEvent(_updateId, _simTime);
			sendClientHandlerEvents(_updateId);
		}
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
	
	private void processInput(StateInputEvent event) {
		// read header (client handler) etc
		int clientHandlerId = event.getClientHandlerId();
		long inputId = event.getInputId();
		_clientHandlerEventTracker.put(clientHandlerId, inputId);
		
		if (!event.isHeartbeat()) {
			// get input bytes
			_stateLogic.collectInput(event.getInputBuffer());
		}
	}
	
	private void sendUpdateEvent(final long updateId, final long simTime) {
		_pubSendQueue.send(PubSubPattern.asTask(_updateEvent, new EventWriter<OutgoingEventHeader, StateUpdateEvent>() {

			@Override
			public void write(OutgoingEventHeader header, StateUpdateEvent event) {
				event.setUpdateId(updateId);
				event.setSimTime(simTime);
				ByteBuffer updateBuffer = event.getUpdateBuffer();
				_stateLogic.createUpdate(updateBuffer);
				event.setUsedLength(updateBuffer);
			}
			
		}));
	}
	
	private void sendClientHandlerEvents(final long updateId) {	
		// Iterate through all client handler IDs and create state update info events until
		// all handler sequence numbers have been published. As we can fit n entries in each
		// update, we attempt to fill each update with the maximum number of entries.
		final IntIterator handlerIdIterator = _clientHandlerEventTracker.keySet().iterator();
		while(handlerIdIterator.hasNext()) {
			_pubSendQueue.send(PubSubPattern.asTask(_updateInfoEvent, new EventWriter<OutgoingEventHeader, StateUpdateInfoEvent>() {
	
				@Override
				public void write(OutgoingEventHeader header, StateUpdateInfoEvent event) {
					int maxEntries = event.getMaximumEntries();
					int currEntryIndex = 0;
					event.setUpdateId(updateId);
					while (handlerIdIterator.hasNext() && currEntryIndex < maxEntries) {
						int handlerId = handlerIdIterator.nextInt();
						long highestSequence = _clientHandlerEventTracker.get(handlerId);
						ClientHandlerEntry entry = new ClientHandlerEntry(handlerId, highestSequence);
						event.setHandlerEntry(currEntryIndex++, entry);
					}
					event.setEntryCount(currEntryIndex + 1);
				}
				
			}));
		}
	}

	@Override
	public String name() {
		return "stateProcessor";
	}


	
}
