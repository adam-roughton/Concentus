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
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.NodeCache;
import com.netflix.curator.framework.recipes.cache.NodeCacheListener;

class ClusterStateNodeListener implements NodeCacheListener, Closeable {

	private final AtomicInteger _lastSeenVersion = new AtomicInteger(-1);
	private final Disruptor<byte[]> _stateUpdateDisruptor;
	private final NodeCache _clusterStateNode;
	
	public ClusterStateNodeListener(
			final CuratorFramework client, 
			final String statePath,
			final Disruptor<byte[]> stateUpdateDisruptor,
			final FatalExceptionCallback exHandler) {
		_clusterStateNode = new NodeCache(client, statePath);
		_clusterStateNode.getListenable().addListener(this);
		_stateUpdateDisruptor = Objects.requireNonNull(stateUpdateDisruptor);
	}

	@Override
	public void nodeChanged() throws Exception {
		ChildData node = _clusterStateNode.getCurrentData();
		if (node != null) {
			int newVersion = node.getStat().getVersion();
			final byte[] data = node.getData();
			
			if (data != null && tryClaimLatestVersion(newVersion)) {
				_stateUpdateDisruptor.publishEvent(new EventTranslator<byte[]>() {
					public void translateTo(byte[] event, long sequence) {
						System.arraycopy(data, 0, event, 0, data.length);
					}
				});
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
		_clusterStateNode.start(false);
	}

	@Override
	public void close() throws IOException {
		_clusterStateNode.close();
	}
	
}
