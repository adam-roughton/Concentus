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

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.NodeCache;
import com.netflix.curator.framework.recipes.cache.NodeCacheListener;

class ClusterServiceStateSignalListener<TState> implements NodeCacheListener, Closeable {

	public interface ListenerDelegate<TState> {
		void onStateChanged(TState newState) throws Exception;
	}
	
	private final Class<TState> _stateType;
	private final AtomicInteger _lastSeenVersion = new AtomicInteger(-1);
	private final ListenerDelegate<TState> _listenerDelegate;
	private final NodeCache _stateSignalNode;
	private final FatalExceptionCallback _exCallback;
	private final Kryo _kryo;
	
	public ClusterServiceStateSignalListener(
			Class<TState> stateType,
			CuratorFramework client, 
			String stateSignalPath,
			ListenerDelegate<TState> listenerDelegate,
			FatalExceptionCallback exCallback) {
		_stateType = Objects.requireNonNull(stateType);
		_stateSignalNode = new NodeCache(client, stateSignalPath);
		_stateSignalNode.getListenable().addListener(this);
		_listenerDelegate = Objects.requireNonNull(listenerDelegate);
		_exCallback = exCallback;
		_kryo = Util.newKryoInstance();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void nodeChanged() throws Exception {
		ChildData node = _stateSignalNode.getCurrentData();
		if (node != null) {
			int newVersion = node.getStat().getVersion();
			final byte[] data = node.getData();
			Input input = new Input(data);
			Object newStateObj = _kryo.readClassAndObject(input);
			input.close();
			
			TState newState;
			if (_stateType.isAssignableFrom(newStateObj.getClass())) {
				newState = (TState) newStateObj;				
			} else {
				newState = null;
			}
			
			if (newState != null && tryClaimLatestVersion(newVersion)) {
				try {
					_listenerDelegate.onStateChanged(newState);
				} catch (Exception e) {
					_exCallback.signalFatalException(e);
				}
			}
		}		
	}
	
	private boolean tryClaimLatestVersion(int newVersion) {
		boolean retry = false;
		boolean isLatest = false;
		do {
			int currentVersion = _lastSeenVersion.get();
			if (newVersion > currentVersion) {
				retry = _lastSeenVersion.compareAndSet(currentVersion, newVersion);
				isLatest = true;
			} else {
				retry = false;
			}
		} while (retry);
		return isLatest;
	}
	
	public void start() throws Exception {
		/*
		 * while the docs for NodeCache report that giving
		 * start the true flag argument calls buildInitial,
		 * it actually calls internalBuildInitial. The difference
		 * between the two is that buildInitial fires a nodeChanged
		 * event on first loading data, while internalBuildInitial
		 * suppresses this event. We want the first state to be processed,
		 * so pass false instead.
		 */
		_stateSignalNode.start(false);
	}

	@Override
	public void close() throws IOException {
		_stateSignalNode.close();
	}
	
}
