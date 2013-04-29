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
	
	private final Clock _clock;
	
	//private final long[] _neighbourJointActionIds = new long[25];
	
	private long _nextSendTimeInMillis = 0;
	private long _lastConfirmedInputId = -1;
	//private long _lastClientUpdateId = -1;
	
	//private long _currentJointActionId = -1;
	private long _clientId = -1;
	private int _handlerId = -1;
	
	private boolean _isActive = false;
	private boolean _isConnecting = false;
	
	public Client(Clock clock) {
		_clock = Objects.requireNonNull(clock);
	}
	
	public boolean isActive() {
		return _isActive;
	}
	
	public void setIsActive(final boolean isActive) {
		_isActive = isActive;
	}
	
	public boolean isConnecting() {
		return _isConnecting;
	}
	
	public void setIsConnecting(final boolean isConnecting) {
		_isConnecting = isConnecting;
	}
	
	public SlidingWindowLongMap getSentIdToSentTimeMap() {
		return _inputIdToSentTimeLookup;
	}
	
	public SlidingWindowLongMap getUpdateIdToRecvTimeMap() {
		return _updateIdToRecvTimeLookup;
	}
	
	public long advanceSendTime() {
		_nextSendTimeInMillis = _clock.currentMillis() + TIME_STEP_IN_MS;
		return _nextSendTimeInMillis;
	}
	
	public long getNextSendTimeInMillis() {
		if (!hasConnected()) {
			return _clock.currentMillis() + 1;
		} else {
			return _nextSendTimeInMillis;
		}
	}
	
	public long getClientId() {
		return _clientId;
	}
	
	public void setClientId(final long clientId) {
		_clientId = clientId;
	}
	
	public int getHandlerId() {
		return _handlerId;
	}
	
	public void setHandlerId(final int handlerId) {
		_handlerId = handlerId;
	}
	
	public boolean hasConnected() {
		return _clientId != -1;
	}
	
	public long getLastConfirmedInputActionId() {
		return _lastConfirmedInputId;
	}
	
	public void setLastConfirmedInputActionId(long inputActionId) {
		_lastConfirmedInputId = inputActionId;
	}

}
