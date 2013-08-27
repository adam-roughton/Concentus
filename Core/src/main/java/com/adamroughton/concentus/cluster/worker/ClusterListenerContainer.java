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
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.KeeperException;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.data.TestRunInfo;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.ClusterPath.*;

public final class ClusterListenerContainer extends SimpleClusterWorkerHandle implements ClusterListenerHandle {
	
	private final Disruptor<byte[]> _stateUpdateDisruptor;
	private final ClusterStateNodeListener _clusterStateNodeListener;
	private final ExecutorService _executor;
	
	public <S extends Enum<S> & ClusterStateValue> ClusterListenerContainer(
			String zooKeeperAddress, 
			String root, 
			ClusterListener<S> clusterListener,
			ExecutorService executor,
			FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, UUID.randomUUID(), clusterListener, executor, exHandler);
	}
	
	@SuppressWarnings("unchecked")
	public <S extends Enum<S> & ClusterStateValue> ClusterListenerContainer(
			String zooKeeperAddress, 
			String root, 
			UUID clusterId,
			ClusterListener<S> clusterListener,
			ExecutorService executor,
			FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, exHandler);
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
	
}
