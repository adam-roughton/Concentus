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
package com.adamroughton.consentus.canonicalstate;

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.Constants;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.ClientHandlerEntry;
import com.adamroughton.consentus.messaging.events.StateInputEvent;
import com.adamroughton.consentus.messaging.events.StateMetricEvent;
import com.adamroughton.consentus.messaging.events.StateUpdateEvent;
import com.adamroughton.consentus.messaging.events.StateUpdateInfoEvent;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class StateProcessor implements EventProcessor {
	
	private final AtomicBoolean _running = new AtomicBoolean(false);
	private final StateInputEvent _inputEvent = new StateInputEvent();
	private final StateUpdateEvent _updateEvent = new StateUpdateEvent();
	private final StateMetricEvent _metricEvent = new StateMetricEvent();
	private final StateUpdateInfoEvent _updateInfoEvent = new StateUpdateInfoEvent();
	
	private final StateLogic _stateLogic;
	private final RingBuffer<byte[]> _inputRingBuffer;
	private final RingBuffer<byte[]> _publishRingBuffer;
	private final FatalExceptionCallback _exCallback;

	private final Sequence _sequence;
	private final SequenceBarrier _barrier;
	
	private final Int2LongMap _clientHandlerEventTracker;
	
	private long _lastTickTime;
	private long _simTime;
	private long _accumulator; // Carry over left over sim time from last update
	
	private long _updateId;
	private long _seqStart;
	private int _eventErrorCount;

	public StateProcessor(
			StateLogic stateLogic,
			RingBuffer<byte[]> inputRingBuffer,
			RingBuffer<byte[]> publishRingBuffer, 
			SequenceBarrier barrier,
			FatalExceptionCallback exceptionCallback,
			Config conf) {
		_stateLogic = Objects.requireNonNull(stateLogic);
		_inputRingBuffer = Objects.requireNonNull(inputRingBuffer);
		_publishRingBuffer = Objects.requireNonNull(publishRingBuffer);
		_exCallback = Objects.requireNonNull(exceptionCallback);

		_clientHandlerEventTracker = new Int2LongOpenHashMap(50);
		
		_barrier = Objects.requireNonNull(barrier);
		_sequence = new Sequence();
	}

	@Override
	public void run() {
		if (!_running.compareAndSet(false, true)) {
			throw new IllegalStateException("The state processor can only be started once.");
		}
		_barrier.clearAlert();
		
		_lastTickTime = System.currentTimeMillis();
		_simTime = 0;
		_updateId = 0;
		_accumulator = 0;
		_seqStart = -1;
		
		long nextSequence = _sequence.get() + 1;
		while(true) {
			try {
				tickIfNeeded();
				
				final long availableSequence = _barrier.waitFor(nextSequence, 
						getTimeUntilNextTick(), TimeUnit.MILLISECONDS);
				while (nextSequence <= availableSequence) {
					byte[] nextInput = _inputRingBuffer.get(nextSequence++);
					processInput(nextInput);
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
		_barrier.alert();
	}
	
	private void tickIfNeeded() {
		long nextTickTime = System.currentTimeMillis();
		if (nextTickTime >= _lastTickTime + Constants.TIME_STEP) {
			long frameTime = nextTickTime - _lastTickTime;
			_accumulator += frameTime;
			
			long dt = Constants.TIME_STEP;
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
		long remainingTime = _lastTickTime + Constants.TIME_STEP - System.currentTimeMillis();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
	private void processInput(byte[] input) {
		// check that the error flag is not set
		if (MessageBytesUtil.readFlagFromByte(input, 0, 0)) {
			_eventErrorCount++;
			return;
		}
		
		_inputEvent.setBackingArray(input, 1);
		// read header (client handler) etc
		int clientHandlerId = _inputEvent.getClientHandlerId();
		long inputId = _inputEvent.getInputId();
		_clientHandlerEventTracker.put(clientHandlerId, inputId);
		
		// get input bytes
		_stateLogic.collectInput(_inputEvent.getInputBuffer());
	}
	
	private void sendUpdateEvent(long updateId, long simTime) {
		long seq = _publishRingBuffer.next();
		byte[] outputBytes = _publishRingBuffer.get(seq);
		try {
			// we reserve the first byte of the buffer as a header
			_updateEvent.setBackingArray(outputBytes, 1);
			_updateEvent.setUpdateId(updateId);
			_updateEvent.setSimTime(simTime);
			_stateLogic.createUpdate(_updateEvent.getUpdateBuffer());
			MessageBytesUtil.writeFlagToByte(outputBytes, 0, 0, false); // is valid
		} catch (Exception e) {
			// indicate error condition on the message
			MessageBytesUtil.writeFlagToByte(outputBytes, 0, 0, true);
			Log.error("An error was raised on receiving a message.", e);
			throw new RuntimeException(e);
		} finally {
			_publishRingBuffer.publish(seq);
			_updateEvent.clear();
		}
	}
	
	private void sendMetricEvent(long updateId, long timeSinceLastInMs) {		
		long seq = _publishRingBuffer.next();
		byte[] outputBytes = _publishRingBuffer.get(seq);
		_metricEvent.setBackingArray(outputBytes, 1);
		_metricEvent.setUpdateId(updateId);
		_metricEvent.setInputActionsProcessed(_sequence.get() - _seqStart);
		_metricEvent.setDurationInMs(timeSinceLastInMs);
		_metricEvent.setEventErrorCount(_eventErrorCount);
		MessageBytesUtil.writeFlagToByte(outputBytes, 0, 0, false); // is valid
		_publishRingBuffer.publish(seq);
		_metricEvent.clear();
	}
	
	private void sendClientHandlerEvents(long updateId) {
		int maximumEntries = 0;
		int nextIndex = 0;
		long seq = -1;
		byte[] outputBytes = null;
		for (int clientHandlerId : _clientHandlerEventTracker.keySet()) {
			if (nextIndex >= maximumEntries) {
				if (outputBytes != null) {
					// publish the last event
					_updateInfoEvent.setEntryCount(maximumEntries);
					_publishRingBuffer.publish(seq);
				}
				seq = _publishRingBuffer.next();
				outputBytes = _publishRingBuffer.get(seq);
				MessageBytesUtil.writeFlagToByte(outputBytes, 0, 0, false); // is valid
				_updateInfoEvent.setBackingArray(outputBytes, 1);
				_updateInfoEvent.setUpdateId(updateId);
				maximumEntries = _updateInfoEvent.getMaximumEntries();
				nextIndex = 0;
			}
			long highestSeq = _clientHandlerEventTracker.get(clientHandlerId);
			ClientHandlerEntry entry = new ClientHandlerEntry(clientHandlerId, highestSeq);
			_updateInfoEvent.setHandlerEntry(nextIndex++, entry);
		}
		if (outputBytes != null) {
			// publish the last event
			_updateInfoEvent.setEntryCount(nextIndex);
			_publishRingBuffer.publish(seq);
		}
		_updateInfoEvent.clear();
	}
}
