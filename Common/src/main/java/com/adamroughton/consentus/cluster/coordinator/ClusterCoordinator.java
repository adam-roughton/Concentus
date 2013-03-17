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
package com.adamroughton.consentus.cluster.coordinator;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.cluster.ClusterParticipant;
import com.adamroughton.consentus.cluster.ClusterState;
import com.adamroughton.consentus.cluster.coordinator.ParticipatingNodes.ParticipatingNodesLatch;
import com.adamroughton.consentus.cluster.worker.ClusterStateValue;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.consentus.cluster.ClusterPath.*;

public final class ClusterCoordinator extends ClusterParticipant implements Cluster, Closeable {

	public ClusterCoordinator(String zooKeeperAddress, String root,
			FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, UUID.randomUUID(), exHandler);
	}
	
	public ClusterCoordinator(String zooKeeperAddress, String root, UUID clusterId,
			FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, exHandler);
	}
	
	@Override
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
	
	@Override
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

	@Override
	public void setAssignment(String serviceType, UUID serviceId,
			byte[] assignment) {
		String serviceTypeAssignmentPath = ZKPaths.makePath(getPath(ASSIGN_RES), serviceType);
		ensurePathCreated(serviceTypeAssignmentPath);
		String serviceAssignmentPath = ZKPaths.makePath(serviceTypeAssignmentPath, serviceId.toString());
		createOrSetEphemeral(serviceAssignmentPath, assignment);
	}

	@Override
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

	@Override	
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
	
	@Override
	public void waitForReady(ParticipatingNodes participatingNodes) throws InterruptedException {		
		waitForReadyInternal(participatingNodes, false, 0, TimeUnit.SECONDS);
	}

	@Override
	public boolean waitForReady(final ParticipatingNodes participatingNodes, long time, TimeUnit unit)
			throws InterruptedException {
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
			getClient().getChildren()
					   .watched()
					   .inBackground(createReadyWaiter(latch, lock, condition))
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
			final Lock lock, final Condition condition) {
		return new BackgroundCallback() {
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event) {
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
						} else {
							getClient().getChildren().watched().inBackground(this).forPath(getPath(READY));
						}
					}
				} catch (Exception e) {
					getExHandler().signalFatalException(e);
				}
			}
		};
	}
	
}
