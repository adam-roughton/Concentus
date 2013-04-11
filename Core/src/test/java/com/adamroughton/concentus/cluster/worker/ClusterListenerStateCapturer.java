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
package com.adamroughton.concentus.cluster.worker;

import java.util.Objects;

import com.adamroughton.concentus.cluster.worker.Cluster;
import com.adamroughton.concentus.cluster.worker.ClusterListener;
import com.adamroughton.concentus.cluster.worker.ClusterStateValue;

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