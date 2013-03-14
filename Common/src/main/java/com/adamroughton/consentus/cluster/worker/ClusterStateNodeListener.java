package com.adamroughton.consentus.cluster.worker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import com.adamroughton.consentus.FatalExceptionCallback;
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
		_stateUpdateDisruptor = Objects.requireNonNull(stateUpdateDisruptor);
	}

	@Override
	public void nodeChanged() throws Exception {
		ChildData node = _clusterStateNode.getCurrentData();
		if (node != null) {
			int newVersion = node.getStat().getVersion();
			final byte[] data = node.getData();
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

			if (isLatest) {
				_stateUpdateDisruptor.publishEvent(new EventTranslator<byte[]>() {
					public void translateTo(byte[] event, long sequence) {
						System.arraycopy(data, 0, event, 0, data.length);
					}
				});
			}
		}		
	}
	
	public void start() throws Exception {
		_clusterStateNode.start(true);
	}

	@Override
	public void close() throws IOException {
		_clusterStateNode.close();
	}
	
}
