package com.adamroughton.concentus.data.cluster.kryo;

import com.adamroughton.concentus.util.Util;

public class StateEntry<TState extends Enum<TState> & ClusterState> {
	
	private TState _state;
	private Object _stateData;
	private int _version;
	private Class<TState> _stateType;
	
	// for Kryo
	@SuppressWarnings("unused")
	private StateEntry() { }
	
	public StateEntry(Class<TState> stateType, TState state, Object stateData, int version) {
		_state = state;
		_stateData = stateData;
		_stateType = stateType;
	}
	
	public TState getState() {
		return _state;
	}
	
	public <TData> TData getStateData(Class<TData> expectedType) {
		return Util.checkedCast(_stateData, expectedType);
	}
	
	/**
	 * Allows the service to declare the version of the signal
	 * that caused the current state entry. Each signal entry
	 * stores a monotonically increasing version number (taken
	 * from the version of the ZooKeeper znode it is stored in).
	 * When services update their state using this signal, they
	 * reuse the version number of the signal in their state entry.
	 * If the state was caused by an internal event to the service, 
	 * this version number should be {@code -1}.
	 * @return the version of this signal entry; the signal entry that
	 * caused this state entry; or -1 if this state entry was caused
	 * by an internal event
	 */
	public int version() {
		return _version;
	}
	
	public Class<TState> stateType() {
		return _stateType;
	}
}