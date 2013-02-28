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
package com.adamroughton.consentus.messaging;

import java.util.Arrays;

import com.adamroughton.consentus.Util;

public final class SocketSettings {

	private final int _socketType;
	private final int[] _portsToBindTo;
	private final String[] _connectionStrings;
	private final long _hwm;
	private final MessagePartBufferPolicy _messagePartPolicy;
	private final int _socketId;
	
	public static SocketSettings create(final int socketType) {
		return new SocketSettings(socketType, new int[0], new String[0], -1, new MessagePartBufferPolicy(0), 0);
	}
	
	private SocketSettings(
			final int socketType,
			final int[] portsToBindTo, 
			final String[] connectionStrings,
			final long hwm,
			final MessagePartBufferPolicy messagePartPolicy,
			final int socketId) {
		_socketType = socketType;
		_portsToBindTo = portsToBindTo;
		_connectionStrings = connectionStrings;
		_hwm = hwm;
		_messagePartPolicy = messagePartPolicy;
		_socketId = socketId;
	}
	
	public SocketSettings bindToPort(final int port) {
		Util.assertPortValid(port);
		int[] portsToBindTo = new int[_portsToBindTo.length + 1];
		System.arraycopy(_portsToBindTo, 0, portsToBindTo, 0, _portsToBindTo.length);
		portsToBindTo[_portsToBindTo.length] = port;
		return new SocketSettings(_socketType, portsToBindTo, _connectionStrings, _hwm, _messagePartPolicy, _socketId);
	}
	
	public SocketSettings connectToAddress(final String address) {
		String[] connectionStrings = new String[_connectionStrings.length + 1];
		System.arraycopy(_connectionStrings, 0, connectionStrings, 0, _connectionStrings.length);
		connectionStrings[_connectionStrings.length] = address;
		return new SocketSettings(_socketType, _portsToBindTo, connectionStrings, _hwm, _messagePartPolicy, _socketId);
	}
	
	public SocketSettings setHWM(final int hwm) {
		if (hwm < 0)
			throw new IllegalArgumentException("The HWM must be 0 or greater.");
		return new SocketSettings(_socketType, _portsToBindTo, _connectionStrings, hwm, _messagePartPolicy, _socketId);
	}
	
	public SocketSettings setMessageOffsets(final int firstOffset, int... subsequentOffsets) {
		int[] offsets = new int[subsequentOffsets.length + 1];
		offsets[0] = firstOffset;
		System.arraycopy(subsequentOffsets, 0, offsets, 1, subsequentOffsets.length);
		MessagePartBufferPolicy messagePartPolicy = new MessagePartBufferPolicy(offsets);
		return new SocketSettings(_socketType, _portsToBindTo, _connectionStrings, _hwm, messagePartPolicy, _socketId);
	}
	
	/**
	 * An optional identifier that will be written in to an attached
	 * buffer as part of a header.
	 * 
	 * @param socketId
	 * @return
	 */
	public SocketSettings setSocketId(final int socketId) {
		return new SocketSettings(_socketType, _portsToBindTo, _connectionStrings, _hwm, _messagePartPolicy, socketId);
	}
	
	public int getSocketType() {
		return _socketType;
	}
	
	public int[] getPortsToBindTo() {
		return Arrays.copyOf(_portsToBindTo, _portsToBindTo.length);
	}
	
	public String[] getConnectionStrings() {
		return Arrays.copyOf(_connectionStrings, _connectionStrings.length);
	}
	
	public long getHWM() {
		return _hwm;
	}
	
	public MessagePartBufferPolicy getMessagePartPolicy() {
		return new MessagePartBufferPolicy(_messagePartPolicy);
	}
	
	/**
	 * An optional identifier that will be written in to an attached
	 * buffer as part of a header.
	 * 
	 * @return the socket ID
	 */
	public int getSocketId() {
		return _socketId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(_connectionStrings);
		result = prime * result + (int) (_hwm ^ (_hwm >>> 32));
		result = prime
				* result
				+ ((_messagePartPolicy == null) ? 0 : _messagePartPolicy
						.hashCode());
		result = prime * result + Arrays.hashCode(_portsToBindTo);
		result = prime * result + _socketId;
		result = prime * result + _socketType;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SocketSettings other = (SocketSettings) obj;
		if (!Arrays.equals(_connectionStrings, other._connectionStrings))
			return false;
		if (_hwm != other._hwm)
			return false;
		if (_messagePartPolicy == null) {
			if (other._messagePartPolicy != null)
				return false;
		} else if (!_messagePartPolicy.equals(other._messagePartPolicy))
			return false;
		if (!Arrays.equals(_portsToBindTo, other._portsToBindTo))
			return false;
		if (_socketId != other._socketId)
			return false;
		if (_socketType != other._socketType)
			return false;
		return true;
	}
	
}
