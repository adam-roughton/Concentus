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

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Objects;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.ClusterState;
import com.lmax.disruptor.EventHandler;

class ClusterStateEventHandler<S extends Enum<S> & ClusterStateValue> implements EventHandler<byte[]> {

	private final ClusterWorkerContainer _cluster;
	private final ClusterListener<S> _listener;
	private final FatalExceptionCallback _exCallback;
	
	private final int _domain;
	private final Int2ObjectMap<S> _stateValueLookup;
	
	public ClusterStateEventHandler(
			final ClusterWorkerContainer cluster,
			final ClusterListener<S> listener,
			final FatalExceptionCallback exCallback) {
		_cluster = Objects.requireNonNull(cluster);
		_listener = Objects.requireNonNull(listener);
		_exCallback = Objects.requireNonNull(exCallback);
		
		S[] values = _listener.getStateValueClass().getEnumConstants();
		_stateValueLookup = new Int2ObjectArrayMap<S>(values.length);
		_domain = values[0].domain();
		for (S value : values) {
			if (value.domain() != _domain) throw new IllegalArgumentException(
					"Only one cluster state domain is supported for a single cluster listener.");
			_stateValueLookup.put(value.code(), value);
		}
	}
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		try {
			ClusterState state = ClusterState.fromBytes(event);
			if (state.getStateDomain() != _domain) {
				_exCallback.signalFatalException(
						new RuntimeException(String.format("The cluster listener " +
								"does not support events of type %d (expected %d)", 
								state.getStateDomain(), _domain)));
			}
			if (!_stateValueLookup.containsKey(state.getStateCode())) {
				_exCallback.signalFatalException(
						new RuntimeException(String.format("No matching ClusterStateValue was found for code %d in type %s.", 
								state.getStateCode(), _listener.getStateValueClass().getName())));
			}
			S newState = _stateValueLookup.get(state.getStateCode());
			_listener.onStateChanged(newState, _cluster);
		} catch (Throwable e) {
			_exCallback.signalFatalException(e);
		}
	}
	
}
