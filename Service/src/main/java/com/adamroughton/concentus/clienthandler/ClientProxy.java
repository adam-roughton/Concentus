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
package com.adamroughton.concentus.clienthandler;

import com.adamroughton.concentus.SlidingWindowLongMap;

public class ClientProxy {

	private final long _clientId;
	private byte[] _clientSocketId;
	private long _lastMsgTime;
	private long _lastUpdateId;
	
	/**
	 * Store the last 128 input actions -> client action ID mappings for this client
	 */
	private final SlidingWindowLongMap _actionIdMap = new SlidingWindowLongMap(128);
	
	public ClientProxy(final long clientId) {
		_clientId = clientId;
		_lastMsgTime = System.nanoTime();
		_clientSocketId = new byte[0];
		_lastUpdateId = -1;
	}
	
	public long getClientId() {
		return _clientId;
	}
	
	public void setSocketId(byte[] socketIdBytes) {
		setSocketId(socketIdBytes, 0, socketIdBytes.length);
	}
	
	public void setSocketId(byte[] socketIdBytes, int offset, int length) {
		if (_clientSocketId.length < length) {
			_clientSocketId = new byte[length];
		}
		System.arraycopy(socketIdBytes, offset, _clientSocketId, 0, length);
	}
	
	public void writeSocketId(byte[] buffer, int offset) {
		System.arraycopy(_clientSocketId, 0, buffer, offset, _clientSocketId.length);
	}
	
	public byte[] getSocketId() {
		byte[] clientSocketId = new byte[_clientSocketId.length];
		System.arraycopy(_clientSocketId, 0, clientSocketId, 0, _clientSocketId.length);
		return clientSocketId;
	}
	
	public long getLastMsgNanoTime() {
		return _lastMsgTime;
	}
	
	public void setLastMsgNanoTime(final long timeInNanos) {
		_lastMsgTime = timeInNanos;
	}
	
	public long getLastUpdateId() {
		return _lastUpdateId;
	}
	
	public void setLastUpdateId(final long updateId) {
		_lastUpdateId = updateId;
	}
	
	
	
	
}
