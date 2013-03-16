package com.adamroughton.consentus.cluster.worker;

import java.util.Objects;

public class ClusterListenerStateCapturer<T extends Enum<T> & ClusterStateValue> 
		implements ClusterListener<T> {

	private final Class<T> _stateType;
	private final ValueCollector<T> _valueCollector;
	
	public ClusterListenerStateCapturer(final Class<T> stateType) {
		_stateType = Objects.requireNonNull(stateType);
		_valueCollector = new ValueCollector<>();
	}
	
	@Override
	public void onStateChanged(T newClusterState, Cluster cluster)
			throws Exception {
		_valueCollector.addValue(newClusterState);
	}
	
	public ValueCollector<T> getValueCollector() {
		return _valueCollector;
	}

	@Override
	public Class<T> getStateValueClass() {
		return _stateType;
	}
	
}