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
package com.adamroughton.concentus.cluster.coordinator;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.WatchedEvent;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.cluster.ClusterState;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes.ParticipatingNodesLatch;
import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.adamroughton.concentus.cluster.data.TestRunInfo;
import com.adamroughton.concentus.cluster.data.WorkerAllocationInfo;
import com.adamroughton.concentus.cluster.worker.ClusterStateValue;
import com.adamroughton.concentus.util.Util;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.ClusterPath.*;

public final class ClusterCoordinatorHandle extends ClusterParticipant implements Closeable {

	public ClusterCoordinatorHandle(String zooKeeperAddress, String root,
			FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, UUID.randomUUID(), exHandler);
	}
	
	public ClusterCoordinatorHandle(String zooKeeperAddress, String root, UUID clusterId,
			FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, exHandler);
	}
	
	public void setRunInfo(TestRunInfo runInfo) {
		createOrSetEphemeral(getPath(RUN_INFO), Util.intToBytes(runInfo.getRunId()));
		createOrSetEphemeral(getPath(RUN_CLIENT_COUNT), Util.intToBytes(runInfo.getClientCount()));
		createOrSetEphemeral(getPath(RUN_DURATION), Util.longToBytes(runInfo.getDuration()));
	}
	
	public void setWorkerAllocations(List<WorkerAllocationInfo> allocations) {
		for (WorkerAllocationInfo allocation : allocations) {
			String path = ZKPaths.makePath(getPath(RUN_WORKER_ALLOCATIONS), allocation.getWorkerId().toString());
			createOrSetEphemeral(path, allocation.getBytes());
		}
	}
	
	public List<String> getAssignmentRequestServiceTypes() {
		List<String> serviceTypes = null;
		try {
			String assignmentRootPath = getPath(ASSIGN_REQ);
			ensurePathCreated(assignmentRootPath);
			serviceTypes = getClient().getChildren().forPath(assignmentRootPath);
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return serviceTypes;
	}
	
	public List<AssignmentRequest> getAssignmentRequests(String serviceType) {
		List<AssignmentRequest> assignmentRequests = null;
		try {
			String serviceAssignmentRoot = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
			ensurePathCreated(serviceAssignmentRoot);
			List<String> serviceIds = getClient().getChildren().forPath(serviceAssignmentRoot);
			assignmentRequests = new ArrayList<>(serviceIds.size());
			for (String serviceId : serviceIds) {
				String path = ZKPaths.makePath(serviceAssignmentRoot, serviceId);
				byte[] reqData = getClient().getData().forPath(path);
				assignmentRequests.add(new AssignmentRequest(serviceId, reqData));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return assignmentRequests;
	}

	public void setAssignment(String serviceType, UUID serviceId,
			byte[] assignment) {
		String serviceTypeAssignmentPath = ZKPaths.makePath(getPath(ASSIGN_RES), serviceType);
		ensurePathCreated(serviceTypeAssignmentPath);
		String serviceAssignmentPath = ZKPaths.makePath(serviceTypeAssignmentPath, serviceId.toString());
		createOrSetEphemeral(serviceAssignmentPath, assignment);
	}

	public void setState(ClusterStateValue state) {
		try {
			ensurePathCreated(getPath(STATE));
			ensurePathCreated(getPath(READY));
			ensurePathCreated(getPath(ASSIGN_REQ));
			
			// delete existing ready flags
			for (String flag : getClient().getChildren().forPath(getPath(READY))) {
				delete(ZKPaths.makePath(getPath(READY), flag));
			}
			
			// delete existing assignment requests
			for (String reqServiceType : getClient().getChildren().forPath(getPath(ASSIGN_REQ))) {
				String reqServiceTypePath = ZKPaths.makePath(getPath(ASSIGN_REQ), reqServiceType);
				for (String req : getClient().getChildren().forPath(reqServiceTypePath)) {
					delete(ZKPaths.makePath(reqServiceTypePath, req));
				}
				delete(reqServiceTypePath);
			}
			
			// update state
			ClusterState newState = new ClusterState(state.domain(), state.code());
			getClient().setData().forPath(getPath(STATE), ClusterState.toBytes(newState));
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
	}

	/**
	 * Takes a snapshot of all of the participating nodes, allowing
	 * them to be tracked for operations such as {@link Cluster#waitForReady(ParticipatingNodes))}.
	 * @return an entity that can be used to track nodes.
	 */
	public ParticipatingNodes getNodeSnapshot() {
		ParticipatingNodes participatingNodes = ParticipatingNodes.create();
		try {
			ensurePathCreated(getPath(READY));
			
			for (String serviceIdString : getClient().getChildren().forPath(getPath(READY))) {
				UUID serviceId = UUID.fromString(serviceIdString);
				participatingNodes = participatingNodes.add(serviceId);
			}
		} catch (Exception e){ 
			getExHandler().signalFatalException(e);
		}		
		return participatingNodes;
	}
	
	public boolean waitForReady(ParticipatingNodes participatingNodes) {		
		return waitForReadyInternal(participatingNodes, false, 0, TimeUnit.SECONDS);
	}

	public boolean waitForReady(final ParticipatingNodes participatingNodes, long time, TimeUnit unit) {
		return waitForReadyInternal(participatingNodes, true, time, unit);
	}
	
	private boolean waitForReadyInternal(ParticipatingNodes participatingNodes, 
			boolean hasTimeOut, long time, TimeUnit unit) {
		final ParticipatingNodesLatch latch = participatingNodes.createNodesLatch();
		
		long startTime = System.nanoTime();
		long remainingTime = unit.toNanos(time);
		
		final Lock lock = new ReentrantLock();
		final Condition condition = lock.newCondition();
		
		try {
			final AtomicBoolean isWaiting = new AtomicBoolean(true);
			final BackgroundCallback readyWaiter = createReadyWaiter(latch, lock, condition);
			final CuratorWatcher readyWatcher = new CuratorWatcher() {
				
				@Override
				public void process(WatchedEvent event) throws Exception {
					if (isWaiting.get()) {
						getClient().getChildren().usingWatcher(this).inBackground(readyWaiter).forPath(getPath(READY));
					}
				}
			};
			getClient().getChildren()
					   .usingWatcher(readyWatcher)
					   .inBackground(readyWaiter)
					   .forPath(getPath(READY));
			
			lock.lock();
			try {
				while (!latch.isDone()) {
					if (hasTimeOut) {
						long elapsedTime = System.nanoTime() - startTime;
						remainingTime -= elapsedTime;
						if (remainingTime > 0) {
							condition.await(elapsedTime, TimeUnit.NANOSECONDS);
						} else {
							isWaiting.set(false);
							return false;
						}
					} else {
						condition.await();
					}
				}
			} finally {
				lock.unlock();
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return true;
	}

	
	private BackgroundCallback createReadyWaiter(final ParticipatingNodesLatch latch,
				final Lock lock, 
				final Condition condition) {
		return new BackgroundCallback() {

			@Override
			public void processResult(CuratorFramework client,
					CuratorEvent event) throws Exception {
				try {
					if (event.getType() == CuratorEventType.CHILDREN) {
						for (String serviceIdString : event.getChildren()) {
							latch.accountFor(UUID.fromString(serviceIdString));
						}
						if (latch.isDone()) {
							lock.lock();
							try {
								condition.signalAll();
							} finally {
								lock.unlock();
							}
						}
					}
				} catch (Exception e) {
					getExHandler().signalFatalException(e);
				}
			}
			
		};
	}
	
	public static class AssignmentRequest {
		private final String _serviceId;
		private final byte[] _requestBytes;
		
		public AssignmentRequest(String serviceId, byte[] requestBytes) {
			_serviceId = Objects.requireNonNull(serviceId);
			_requestBytes = Objects.requireNonNull(requestBytes);
		}
		
		public String getServiceId() {
			return _serviceId;
		}
		
		public byte[] getRequestBytes() {
			byte[] copy = new byte[_requestBytes.length];
			System.arraycopy(_requestBytes, 0, copy, 0, copy.length);
			return copy;
		}
	}
	
	public static class MetricRegistrationRequest {
		private final UUID _serviceId;
		private final MetricPublisherInfo _info;
		
		public MetricRegistrationRequest(UUID serviceId, MetricPublisherInfo info) {
			_serviceId = Objects.requireNonNull(serviceId);
			_info = Objects.requireNonNull(info);
		}
		
		public MetricRegistrationRequest(String serviceId, MetricPublisherInfo info) {
			this(UUID.fromString(serviceId), info);
		}
		
		public UUID getServiceId() {
			return _serviceId;
		}
		
		public MetricPublisherInfo getInfo() {
			return _info;
		}
	}
}
