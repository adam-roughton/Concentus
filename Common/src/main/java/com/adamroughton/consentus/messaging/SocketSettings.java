package com.adamroughton.consentus.messaging;

import java.util.Arrays;

import com.adamroughton.consentus.Util;

public final class SocketSettings {

	private final int _socketType;
	private final int[] _portsToBindTo;
	private final String[] _connectionStrings;
	private final long _hwm;
	private final int[] _messageOffsets;
	
	public static SocketSettings create(final int socketType) {
		return new SocketSettings(socketType, new int[0], new String[0], -1, new int[] { 0 });
	}
	
	private SocketSettings(
			final int socketType,
			final int[] portsToBindTo, 
			final String[] connectionStrings,
			final long hwm,
			final int[] messageOffsets) {
		_socketType = socketType;
		_portsToBindTo = portsToBindTo;
		_connectionStrings = connectionStrings;
		_hwm = hwm;
		_messageOffsets = messageOffsets;
	}
	
	public SocketSettings bindToPort(final int port) {
		Util.assertPortValid(port);
		int[] portsToBindTo = new int[_portsToBindTo.length + 1];
		System.arraycopy(_portsToBindTo, 0, portsToBindTo, 0, _portsToBindTo.length);
		portsToBindTo[_portsToBindTo.length] = port;
		return new SocketSettings(_socketType, portsToBindTo, _connectionStrings, _hwm, _messageOffsets);
	}
	
	public SocketSettings connectToAddress(final String address) {
		String[] connectionStrings = new String[_connectionStrings.length + 1];
		System.arraycopy(_connectionStrings, 0, connectionStrings, 0, _connectionStrings.length);
		connectionStrings[_connectionStrings.length] = address;
		return new SocketSettings(_socketType, _portsToBindTo, connectionStrings, _hwm, _messageOffsets);
	}
	
	public SocketSettings setHWM(final int hwm) {
		if (hwm < 0)
			throw new IllegalArgumentException("The HWM must be 0 or greater.");
		return new SocketSettings(_socketType, _portsToBindTo, _connectionStrings, hwm, _messageOffsets);
	}
	
	public SocketSettings setMessageOffsets(final int firstOffset, int... subsequentOffsets) {
		int[] offsets = new int[subsequentOffsets.length + 1];
		for (int i = 0; i < subsequentOffsets.length + 1; i++) {
			int offset;
			if (i == 0) {
				offset = firstOffset;
			} else {
				offset = subsequentOffsets[i - 1];
			}
			if (offset < 0) {
				throw new IllegalArgumentException("The offset must be 0 or greater.");
			} else {
				offsets[i] = offset;
			}
		}
		return new SocketSettings(_socketType, _portsToBindTo, _connectionStrings, _hwm, offsets);
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
	
	public int[] getMessageOffsets() {
		return Arrays.copyOf(_messageOffsets, _messageOffsets.length);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(_connectionStrings);
		result = prime * result + (int) (_hwm ^ (_hwm >>> 32));
		result = prime * result + Arrays.hashCode(_messageOffsets);
		result = prime * result + Arrays.hashCode(_portsToBindTo);
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
		if (!Arrays.equals(_messageOffsets, other._messageOffsets))
			return false;
		if (!Arrays.equals(_portsToBindTo, other._portsToBindTo))
			return false;
		if (_socketType != other._socketType)
			return false;
		return true;
	}
	
}
