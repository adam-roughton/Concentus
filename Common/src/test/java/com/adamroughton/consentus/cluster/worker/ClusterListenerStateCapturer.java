package com.adamroughton.consentus.cluster.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ClusterListenerStateCapturer<T extends Enum<T> & ClusterStateValue> 
		implements ClusterListener<T> {

	private final Class<T> _stateType;
	private final List<T> _stateChanges;
	
	public ClusterListenerStateCapturer(final Class<T> stateType) {
		_stateType = Objects.requireNonNull(stateType);
		_stateChanges = new ArrayList<>();
	}
	
	@Override
	public synchronized void onStateChanged(T newClusterState, Cluster cluster)
			throws Exception {
		_stateChanges.add(newClusterState);
	}

	@Override
	public Class<T> getStateValueClass() {
		return _stateType;
	}
	
	public synchronized List<T> getCapturedStates() {
		return new ArrayList<>(_stateChanges);
	}
	
}