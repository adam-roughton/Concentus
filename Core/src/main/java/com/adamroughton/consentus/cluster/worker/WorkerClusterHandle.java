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
package com.adamroughton.consentus.cluster.worker;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.KeeperException;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.cluster.ClusterParticipant;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.consentus.cluster.ClusterPath.*;

public final class WorkerClusterHandle extends ClusterParticipant implements Cluster, Closeable {
	
	private final Set<String> _hostedServices;
	
	private final Disruptor<byte[]> _stateUpdateDisruptor;
	private final ClusterStateNodeListener _clusterStateNodeListener;
	private final ExecutorService _executor;
	
	public <S extends Enum<S> & ClusterStateValue> WorkerClusterHandle(
			final String zooKeeperAddress, 
			final String root, 
			final ClusterListener<S> clusterListener,
			final ExecutorService executor,
			final FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, UUID.randomUUID(), clusterListener, executor, exHandler);
	}
	
	@SuppressWarnings("unchecked")
	public <S extends Enum<S> & ClusterStateValue> WorkerClusterHandle(
			final String zooKeeperAddress, 
			final String root, 
			final UUID clusterId,
			final ClusterListener<S> clusterListener,
			final ExecutorService executor,
			final FatalExceptionCallback exHandler) {
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
	
	public void registerService(final String serviceType, final String address) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		createServiceNode(serviceTypePath, address.getBytes());
		_hostedServices.add(serviceTypePath);
	}
	
	public void unregisterService(final String serviceType) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		deleteServiceNode(serviceTypePath);
		_hostedServices.remove(serviceTypePath);
	}
	
	public String getServiceAtRandom(final String serviceType) {
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
	
	public String[] getAllServices(final String serviceType) {
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
	
	private List<String> getAvailableServiceIds(final String serviceType) throws Exception {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		ensurePathCreated(serviceTypePath);
		return getClient().getChildren().forPath(serviceTypePath);
	}
	
	private String getServiceAddress(final String serviceType, final String serviceId) throws Exception {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		String servicePath = ZKPaths.makePath(serviceTypePath, serviceId);
		return new String(getClient().getData().forPath(servicePath));
	}
	
	public void requestAssignment(final String serviceType, final byte[] requestBytes) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
		createServiceNode(serviceTypeBase, requestBytes);
	}
	
	public byte[] getAssignment(final String serviceType) {
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
	
	public void deleteAssignmentRequest(final String serviceType) {
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
	private void createServiceNode(final String parentPath, final byte[] data) {
		ensurePathCreated(parentPath);
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.createOrSetEphemeral(servicePath, data);
	}
	
	/**
	 * Deletes an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 */
	private void deleteServiceNode(final String parentPath) {
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.delete(servicePath);
	}
	
}
