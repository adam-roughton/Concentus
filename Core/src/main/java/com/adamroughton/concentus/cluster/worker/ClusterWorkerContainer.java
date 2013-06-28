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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.adamroughton.concentus.cluster.data.TestRunInfo;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.ClusterPath.*;

public final class ClusterWorkerContainer extends ClusterParticipant implements ClusterWorkerHandle, Closeable {
	
	private final Set<String> _hostedServices;
	
	private final Disruptor<byte[]> _stateUpdateDisruptor;
	private final ClusterStateNodeListener _clusterStateNodeListener;
	private final ExecutorService _executor;
	
	public <S extends Enum<S> & ClusterStateValue> ClusterWorkerContainer(
			String zooKeeperAddress, 
			String root, 
			ClusterListener<S> clusterListener,
			ExecutorService executor,
			FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, UUID.randomUUID(), clusterListener, executor, exHandler);
	}
	
	@SuppressWarnings("unchecked")
	public <S extends Enum<S> & ClusterStateValue> ClusterWorkerContainer(
			String zooKeeperAddress, 
			String root, 
			UUID clusterId,
			ClusterListener<S> clusterListener,
			ExecutorService executor,
			FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, exHandler);
		_hostedServices = new HashSet<>(1);
		_executor = Objects.requireNonNull(executor);
		
		_stateUpdateDisruptor = new Disruptor<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[8];
			}
		}, 16, _executor);
		
		_stateUpdateDisruptor.handleEventsWith(
				new ClusterStateEventHandler<S>(this, clusterListener, getExHandler()));
		
		_clusterStateNodeListener = new ClusterStateNodeListener(
				getClient(), 
				getPath(STATE), 
				_stateUpdateDisruptor, 
				getExHandler());
	}
	
	@Override
	public void start() throws Exception {
		super.start();
		_stateUpdateDisruptor.start();
		_clusterStateNodeListener.start();
	}
	
	@Override
	public void close() throws IOException {
		for (String serviceBasePath : _hostedServices) {
			delete(serviceBasePath);
		}
		super.close();
	}
	
	@Override
	public TestRunInfo getCurrentRunInfo() {
		try {
			try {
				int runId = Util.bytesToInt(getClient().getData().forPath(getPath(RUN_INFO)));
				int clientCount = Util.bytesToInt(getClient().getData().forPath(getPath(RUN_CLIENT_COUNT)));
				long duration = Util.bytesToLong(getClient().getData().forPath(getPath(RUN_DURATION)));
				return new TestRunInfo(runId, clientCount, duration);
			} catch (KeeperException eKeeper) {
				if (eKeeper.code() == KeeperException.Code.NONODE) {
					return null;
				} else {
					throw new RuntimeException(eKeeper);
				}
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
			return null;
		}
	}
	
	@Override
	public void registerAsMetricPublisher(String type, String pubAddress, String metaDataReqAddress) {
		UUID serviceId = getMyId();
		MetricPublisherInfo info = new MetricPublisherInfo(serviceId, type, pubAddress, metaDataReqAddress);	
		createServiceNode(getPath(METRIC_PUBLISHERS), info.getBytes());
	}
	
	@Override
	public List<MetricPublisherInfo> getMetricPublishers() {
		return getMetricPublishers(new CuratorWatcher() {

			@Override
			public void process(WatchedEvent event) throws Exception {
			}
			
		});
	}
	
	@Override
	public List<MetricPublisherInfo> getMetricPublishers(CuratorWatcher watcher) {
		List<MetricPublisherInfo> metricPublishers = null;
		try {
			ensurePathCreated(getPath(METRIC_PUBLISHERS));
			List<String> serviceIds = getClient().getChildren().usingWatcher(watcher).forPath(getPath(METRIC_PUBLISHERS));
			metricPublishers = new ArrayList<>(serviceIds.size());
			for (String serviceId : serviceIds) {
				String path = ZKPaths.makePath(getPath(METRIC_PUBLISHERS), serviceId);
				MetricPublisherInfo info = MetricPublisherInfo.fromBytes(getClient().getData().forPath(path));
				metricPublishers.add(info);
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return metricPublishers;
	}
	
	public void registerService(String serviceType, String address) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		createServiceNode(serviceTypePath, address.getBytes());
		_hostedServices.add(serviceTypePath);
	}
	
	public void unregisterService(String serviceType) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		deleteServiceNode(serviceTypePath);
		_hostedServices.remove(serviceTypePath);
	}
	
	public String getServiceAtRandom(String serviceType) {
		String service = null;
		try {
			List<String> serviceNodes = getAvailableServiceIds(serviceType);
			if (!serviceNodes.isEmpty()) {
				int index = (int) Math.round(Math.random() * (serviceNodes.size() - 1));
				service = getServiceAddress(serviceType, serviceNodes.get(index));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return service;
	}
	
	public String[] getAllServices(String serviceType) {
		String[] services = null;
		try {
			List<String> serviceNodes = getAvailableServiceIds(serviceType);
			services = new String[serviceNodes.size()];
			for (int i = 0; i < serviceNodes.size(); i++) {
				services[i] = getServiceAddress(serviceType, serviceNodes.get(i));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return services;
	}
	
	private List<String> getAvailableServiceIds(String serviceType) throws Exception {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		ensurePathCreated(serviceTypePath);
		return getClient().getChildren().forPath(serviceTypePath);
	}
	
	private String getServiceAddress(String serviceType, String serviceId) throws Exception {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		String servicePath = ZKPaths.makePath(serviceTypePath, serviceId);
		return new String(getClient().getData().forPath(servicePath));
	}
	
	public void requestAssignment(String serviceType, byte[] requestBytes) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
		createServiceNode(serviceTypeBase, requestBytes);
	}
	
	public byte[] getAssignment(String serviceType) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_RES), serviceType);
		String serviceAssignmentPath = ZKPaths.makePath(serviceTypeBase, getMyIdString());
		byte[] assignment = null;
		try {
			ensurePathCreated(serviceTypeBase);
			assignment = getClient().getData().forPath(serviceAssignmentPath);
		} catch (KeeperException eKeeper) {
			if (eKeeper.code() == KeeperException.Code.NONODE) {
				return null;
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return assignment;
	}
	
	public void deleteAssignmentRequest(String serviceType) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
		deleteServiceNode(serviceTypeBase);
	}
	
	public void signalReady() {
		createServiceNode(getPath(READY), new byte[0]);
	}
	
	/**
	 * Creates an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 * @param data
	 */
	private void createServiceNode(String parentPath, byte[] data) {
		ensurePathCreated(parentPath);
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.createOrSetEphemeral(servicePath, data);
	}
	
	/**
	 * Deletes an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 */
	private void deleteServiceNode(String parentPath) {
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.delete(servicePath);
	}
	
}
