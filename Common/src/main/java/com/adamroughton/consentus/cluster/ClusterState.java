package com.adamroughton.consentus.cluster;

import java.util.Objects;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class ClusterState {

	private final int _stateDomain;
	private final int _stateCode;
	
	public ClusterState(final int stateDomain, final int stateCode) {
		_stateDomain = stateDomain;
		_stateCode = stateCode;
	}

	public int getStateDomain() {
		return _stateDomain;
	}

	public int getStateCode() {
		return _stateCode;
	}
	
	public static ClusterState fromBytes(byte[] stateBytes) {
		Objects.requireNonNull(stateBytes);
		if (stateBytes.length < 8) 
			throw new IllegalArgumentException(
					String.format("The byte array is too small to store a cluster state (%d < 8).", 
							stateBytes.length));
		int stateDomain = MessageBytesUtil.readInt(stateBytes, 0);
		int stateCode = MessageBytesUtil.readInt(stateBytes, 4);
		return new ClusterState(stateDomain, stateCode);
	}
	
	public static byte[] toBytes(ClusterState state) {
		byte[] stateBytes = new byte[8];
		MessageBytesUtil.writeInt(stateBytes, 0, state.getStateDomain());
		MessageBytesUtil.writeInt(stateBytes, 4, state.getStateCode());
		return stateBytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _stateCode;
		result = prime * result + _stateDomain;
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
		ClusterState other = (ClusterState) obj;
		if (_stateCode != other._stateCode)
			return false;
		if (_stateDomain != other._stateDomain)
			return false;
		return true;
	}
	
}
