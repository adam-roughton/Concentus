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
package com.adamroughton.concentus.crowdhammer.worker;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientConnectEvent;
import com.adamroughton.concentus.messaging.events.ClientInputEvent;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.StatsMetric;
import com.adamroughton.concentus.util.SlidingWindowLongMap;
import com.adamroughton.concentus.util.Util;

import static com.adamroughton.concentus.Constants.TIME_STEP_IN_MS;

public final class Client {

	/**
	 * Buffer 10 seconds worth of sent actions or received update time stamps, count as no response
	 * if not received within this window.
	 */
	public final static int WINDOW_SIZE = Util.nextPowerOf2((int)(10000 / TIME_STEP_IN_MS));	
	
	private final SlidingWindowLongMap _inputIdToSentTimeLookup = new SlidingWindowLongMap(WINDOW_SIZE);
	private final SlidingWindowLongMap _updateIdToRecvTimeLookup = new SlidingWindowLongMap(WINDOW_SIZE);
	
	private final long _index;
	private final Clock _clock;
	
	private final ClientConnectEvent _connectEvent = new ClientConnectEvent();
	private final ClientInputEvent _inputEvent = new ClientInputEvent();
	
	//private final long[] _neighbourJointActionIds = new long[25];
	
	private long _lastActionTime = -1;
	private long _lastConfirmedInputId = -1;
	//private long _lastClientUpdateId = -1;
	
	private long _clientId = -1;
	private int _handlerId = -1;
	
	private boolean _isActive = false;
	private boolean _isConnecting = false;
	
	public Client(long index, Clock clock) {
		_index = index;
		_clock = Objects.requireNonNull(clock);
	}
	
	public boolean isActive() {
		return _isActive;
	}
	
	public void setIsActive(boolean isActive) {
		_isActive = isActive;
	}
	
	public int getHandlerId() {
		return _handlerId;
	}
	
	public void setHandlerId(int handlerId) {
		_handlerId = handlerId;
	}
	
	public long getNextDeadline() {
		if (!hasConnected()) {
			return _clock.currentMillis();
		} else {
			return _lastActionTime + TIME_STEP_IN_MS;
		}
	}
	
	public long getClientId() {
		return _clientId;
	}
	
	private boolean hasConnected() {
		return _clientId != -1;
	}
	
	public void onActionDeadline(SendQueue<OutgoingEventHeader> clientSendQueue, CountMetric sentActionThroughputMetric) {
		if (_clientId == -1) {
			// if we are waiting to connect, do nothing with this client
			if (_isConnecting) return;
			connect(clientSendQueue);
			_isConnecting = true;
		} else {
			sendInputAction(clientSendQueue, sentActionThroughputMetric);
		}
		_lastActionTime = _clock.currentMillis();
	}
	
	private void sendInputAction(SendQueue<OutgoingEventHeader> clientSendQueue, CountMetric sentActionThroughputMetric) {
		clientSendQueue.send(EventPattern.asTask(_inputEvent, new EventWriter<OutgoingEventHeader, ClientInputEvent>() {

			@Override
			public void write(OutgoingEventHeader header, ClientInputEvent event) throws Exception {
				header.setTargetSocketId(event.getBackingArray(), _handlerId);
				long sendTime = _clock.currentMillis();
				long actionId = _inputIdToSentTimeLookup.add(sendTime);
				event.setClientId(_clientId);
				event.setClientActionId(actionId);
				event.setUsedLength(event.getInputBuffer());
			}
			
		}));
		sentActionThroughputMetric.push(1);
	}
	
	private void connect(SendQueue<OutgoingEventHeader> clientSendQueue) {
		clientSendQueue.send(EventPattern.asTask(_connectEvent, new EventWriter<OutgoingEventHeader, ClientConnectEvent>() {

			@Override
			public void write(OutgoingEventHeader header, ClientConnectEvent event) throws Exception {
				header.setTargetSocketId(event.getBackingArray(), _handlerId);
				event.setCallbackBits(_index);
			}
			
		}));
	}
	
	public void onClientUpdate(ClientUpdateEvent updateEvent, StatsMetric actionEffectLatencyMetric, CountMetric lateActionEffectMetric) {
		long updateRecvTime = _clock.currentMillis();
		_updateIdToRecvTimeLookup.put(updateEvent.getUpdateId(), updateRecvTime);
		
		// work out the latency for any sent input
		long lastConfirmedInputId = _lastConfirmedInputId;
		for (long inputId = lastConfirmedInputId + 1; inputId <= updateEvent.getHighestInputActionId(); inputId++) {
			if (_inputIdToSentTimeLookup.containsIndex(inputId)) {
				long latency = updateRecvTime - _inputIdToSentTimeLookup.getDirect(inputId);
				actionEffectLatencyMetric.push(latency);
				_inputIdToSentTimeLookup.remove(inputId);
			} else {
				lateActionEffectMetric.push(1);
			}
		}
		_lastConfirmedInputId = updateEvent.getHighestInputActionId();
	}
	
	public void onConnectResponse(ConnectResponseEvent connectResEvent, CountMetric clientCountMetric) {
		if (!_isConnecting) 
			throw new RuntimeException(String.format("Expected the client (index = %d) to be connecting on reception of a connect response event.", _index));
		if (connectResEvent.getResponseCode() != ConnectResponseEvent.RES_OK) {
			throw new RuntimeException(String.format("The response code for a client connection was %d, expected %d (OK). Aborting test", 
					connectResEvent.getResponseCode(), ConnectResponseEvent.RES_OK));
		}
		_clientId = connectResEvent.getClientIdBits();
		_isConnecting = false;
		clientCountMetric.push(1);
	}
	
	public void reset() {
		_isConnecting = false;
		_clientId = -1;
		_isActive = false;
		_inputIdToSentTimeLookup.clear();
		_updateIdToRecvTimeLookup.clear();
	}

}
