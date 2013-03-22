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
package com.adamroughton.consentus.crowdhammer.worker;

import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.Constants;
import com.adamroughton.consentus.Util;

public final class Client {

	/**
	 * Buffer 10 seconds worth of send actions time stamps, count as no response
	 * if not received within this window.
	 */
	public final static int SEND_BUFFER_SIZE = Util.nextPowerOf2((int)(10000 / Constants.TIME_STEP_IN_MS));
	private final static int SEND_BUFFER_MASK = SEND_BUFFER_SIZE - 1;
	
	private final static long TIME_STEP_IN_NANOS = TimeUnit.MILLISECONDS.toNanos(Constants.TIME_STEP_IN_MS);
	
	private final ZMQ.Socket _socket;
	//private final long[] _neighbourJointActionIds = new long[25];
	
	private long _nextSendTimeInNanos = 0;
	//private long _lastClientUpdateId = -1;
	
	private long _currentInputAction = -1;
	private int _currentInputActionCursor = -1;
	private long[] _inputActionSendTimes = new long[SEND_BUFFER_SIZE];
	
	//private long _currentJointActionId = -1;
	private long _clientId = -1;
	private String _clientHandlerConnString;
	
	private boolean _isActive = false;
	
	public Client(ZMQ.Context zmqContext) {
		_socket = zmqContext.socket(ZMQ.DEALER);
	}
	
	public ZMQ.Socket getSocket() {
		return _socket;
	}
	
	public boolean isActive() {
		return _isActive;
	}
	
	public void setIsActive(final boolean isActive) {
		_isActive = isActive;
	}
	
	public long getSentTime(long inputActionId) {
		int relIndex = (int) (_currentInputAction - inputActionId);
		if (outOfActionRange(relIndex)) {
			return -1;
		}
		return _inputActionSendTimes[(_currentInputActionCursor - relIndex) & SEND_BUFFER_MASK];
	}
	
	public long addSentAction(long sentTime) {
		_currentInputAction++;
		_currentInputActionCursor++;
		_currentInputActionCursor &= SEND_BUFFER_MASK;
		_inputActionSendTimes[_currentInputActionCursor] = sentTime;
		return _currentInputAction;
	}
	
	public boolean outOfActionRange(long relIndex) {
		return relIndex < 0 || 
				relIndex >= _inputActionSendTimes.length;
	}
	
	public long advanceSendTime() {
		_nextSendTimeInNanos = System.nanoTime() + TIME_STEP_IN_NANOS;
		return _nextSendTimeInNanos;
	}
	
	public long getNextSendTimeInNanos() {
		return _nextSendTimeInNanos;
	}
	
	public long getClientId() {
		return _clientId;
	}
	
	public void setClientId(final long clientId) {
		_clientId = clientId;
	}
	
	public String getClientHandlerConnString() {
		return _clientHandlerConnString;
	}
	
	public void setClientHandlerConnString(final String connString) {
		_clientHandlerConnString = connString;
	}
	
	public boolean hasConnected() {
		return _clientId != -1;
	}
}
