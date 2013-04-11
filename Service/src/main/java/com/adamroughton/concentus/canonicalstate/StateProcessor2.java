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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientHandlerEntry;
import com.adamroughton.concentus.messaging.events.StateInputEvent;
import com.adamroughton.concentus.messaging.events.StateMetricEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateInfoEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class StateProcessor2 implements EventProcessor {
	
	private final AtomicBoolean _running = new AtomicBoolean(false);
	private final StateInputEvent _inputEvent = new StateInputEvent();
	private final StateUpdateEvent _updateEvent = new StateUpdateEvent();
	private final StateMetricEvent _metricEvent = new StateMetricEvent();
	private final StateUpdateInfoEvent _updateInfoEvent = new StateUpdateInfoEvent();
	
	private final StateLogic _stateLogic;
	private final RingBuffer<byte[]> _inputRingBuffer;
	private final SequenceBarrier _inputBarrier;
	private final IncomingEventHeader _subHeader;
	private final SendQueue<OutgoingEventHeader> _pubSendQueue;
	private final FatalExceptionCallback _exCallback;

	private final Sequence _sequence;
	
	private final Int2LongMap _clientHandlerEventTracker;
	
	private long _lastTickTime;
	private long _simTime;
	private long _accumulator; // Carry over left over sim time from last update
	
	private long _updateId;
	private long _seqStart;
	private int _eventErrorCount;

	public StateProcessor2(
			StateLogic stateLogic,
			RingBuffer<byte[]> inputRingBuffer,
			SequenceBarrier inputBarrier,
			IncomingEventHeader subHeader,
			SendQueue<OutgoingEventHeader> pubSendQueue, 
			FatalExceptionCallback exceptionCallback) {
		_stateLogic = Objects.requireNonNull(stateLogic);
		_inputRingBuffer = Objects.requireNonNull(inputRingBuffer);
		_inputBarrier = Objects.requireNonNull(inputBarrier);
		_subHeader = Objects.requireNonNull(subHeader);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		_exCallback = Objects.requireNonNull(exceptionCallback);

		_clientHandlerEventTracker = new Int2LongOpenHashMap(50);
		
		_sequence = new Sequence();
	}

	@Override
	public void run() {
		if (!_running.compareAndSet(false, true)) {
			throw new IllegalStateException("The state processor can only be started once.");
		}
		_inputBarrier.clearAlert();
		
		_lastTickTime = System.currentTimeMillis();
		_simTime = 0;
		_updateId = 0;
		_accumulator = 0;
		_seqStart = -1;
		
		long nextSequence = _sequence.get() + 1;
		while(true) {
			try {
				tickIfNeeded();
				
				final long availableSequence = _inputBarrier.waitFor(nextSequence, 
						getTimeUntilNextTick(), TimeUnit.MILLISECONDS);
				while (nextSequence <= availableSequence) {
					byte[] nextInput = _inputRingBuffer.get(nextSequence++);
					boolean wasValid = _subHeader.isValid(nextInput);
					if (wasValid) {
						EventPattern.readContent(nextInput, _subHeader, _inputEvent, new EventReader<IncomingEventHeader, StateInputEvent>() {

							@Override
							public void read(IncomingEventHeader header, StateInputEvent event) {
								processInput(event);
							}
							
						});
					} else {
						_eventErrorCount++;
					}
				}
				_sequence.set(availableSequence);
				
			} catch (final AlertException eAlert) {
				if (!_running.get()) {
					break;
				}
			} catch (final Throwable e) {
				_exCallback.signalFatalException(e);
			}
		}
	}
	
	@Override
	public Sequence getSequence() {
		return _sequence;
	}

	@Override
	public void halt() {
		_running.set(false);
		_inputBarrier.alert();
	}
	
	private void tickIfNeeded() {
		long nextTickTime = System.currentTimeMillis();
		if (nextTickTime >= _lastTickTime + Constants.TIME_STEP_IN_MS) {
			long frameTime = nextTickTime - _lastTickTime;
			_accumulator += frameTime;
			
			long dt = Constants.TIME_STEP_IN_MS;
			while (_accumulator >= dt) {
				_stateLogic.tick(_simTime, dt);
				_simTime += dt;
				_accumulator -= dt;
			}
			_lastTickTime = nextTickTime;
			
			_updateId++;
			sendUpdateEvent(_updateId, _simTime);
			sendMetricEvent(_updateId, frameTime);
			sendClientHandlerEvents(_updateId);
			
			_seqStart = _sequence.get();
			_eventErrorCount = 0;
			_clientHandlerEventTracker.clear();
		}
	}
	
	private long getTimeUntilNextTick() {
		long remainingTime = _lastTickTime + Constants.TIME_STEP_IN_MS - System.currentTimeMillis();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
	private void processInput(StateInputEvent event) {
		// read header (client handler) etc
		int clientHandlerId = event.getClientHandlerId();
		long inputId = event.getInputId();
		_clientHandlerEventTracker.put(clientHandlerId, inputId);
		
		// get input bytes
		_stateLogic.collectInput(event.getInputBuffer());
	}
	
	private void sendUpdateEvent(final long updateId, final long simTime) {
		_pubSendQueue.send(PubSubPattern.asTask(_updateEvent, new EventWriter<OutgoingEventHeader, StateUpdateEvent>() {

			@Override
			public void write(OutgoingEventHeader header, StateUpdateEvent event) {
				event.setUpdateId(updateId);
				event.setSimTime(simTime);
				ByteBuffer updateBuffer = event.getUpdateBuffer();
				_stateLogic.createUpdate(updateBuffer);
				event.addUsedLength(updateBuffer);
			}
			
		}));
	}
	
	private void sendMetricEvent(final long updateId, final long timeSinceLastInMs) {
		_pubSendQueue.send(PubSubPattern.asTask(_metricEvent, new EventWriter<OutgoingEventHeader, StateMetricEvent>() {

			@Override
			public void write(OutgoingEventHeader header, StateMetricEvent event) {
				event.setUpdateId(updateId);
				event.setInputActionsProcessed(_sequence.get() - _seqStart);
				event.setActualBucketDurationInMs(timeSinceLastInMs);
				event.setEventErrorCount(_eventErrorCount);
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
	
	
}
