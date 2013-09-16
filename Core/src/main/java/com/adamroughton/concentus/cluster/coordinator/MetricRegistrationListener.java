package com.adamroughton.concentus.cluster.coordinator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException.NoNodeException;

import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.util.IdentityWrapper;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;

final class MetricRegistrationListener implements PathChildrenCacheListener, Closeable {

	private final PathChildrenCache _metricsCache;
	private final Kryo _kryo;
	
	private final Set<IdentityWrapper<MetricRegistrationCallback>> _listeners = new HashSet<>();
	
	public MetricRegistrationListener(ClusterParticipant clusterHandle) {
		String path = clusterHandle.resolvePathFromRoot(CorePath.METRICS);
		
		_kryo = Util.newKryoInstance();
		_metricsCache = new PathChildrenCache(clusterHandle.getClient(), path, false);
		_metricsCache.getListenable().addListener(this);
	}
	
	public void start() throws Exception {
		_metricsCache.start();
	}
	
	public void close() throws IOException {
		_metricsCache.close();
	}
	
	public void addListener(MetricRegistrationCallback listener) {
		synchronized (_listeners) {
			_listeners.add(new IdentityWrapper<>(listener));
		}
	}
	
	public boolean removeListener(MetricRegistrationCallback listener) {
		synchronized (_listeners) {
			return _listeners.remove(new IdentityWrapper<>(listener));
		}
	}

	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
			throws Exception {
		if (event.getType() == Type.CHILD_ADDED) {
			String metricPath = event.getData().getPath();
			try {
				byte[] metricMetaDataBytes = client.getData().forPath(metricPath);
				MetricMetaData metricMetaData = Util.fromKryoBytes(_kryo, metricMetaDataBytes, MetricMetaData.class);
				List<MetricRegistrationCallback> listeners;
				synchronized (_listeners) {
					listeners = new ArrayList<>(_listeners.size());
					for (IdentityWrapper<MetricRegistrationCallback> listenerWrapper : _listeners) {
						listeners.add(listenerWrapper.get());
					}
				}
				for (MetricRegistrationCallback listener : listeners) {
					listener.newMetric(metricMetaData);
				}
			} catch (NoNodeException ignoreDeletedMetric) {
			}
		}
	}
	
}
