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
package com.adamroughton.consentus.crowdhammer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Objects;

import com.adamroughton.consentus.clienthandler.ClientHandlerService;
import com.adamroughton.consentus.cluster.coordinator.Cluster.AssignmentRequest;
import com.adamroughton.consentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.consentus.cluster.coordinator.ParticipatingNodes;
import com.adamroughton.consentus.crowdhammer.worker.WorkerService;
import com.adamroughton.consentus.messaging.MessageBytesUtil;

import static com.adamroughton.consentus.crowdhammer.CrowdHammerServiceState.*;

public final class TestRunner implements Runnable {

	private final static Logger LOG = Logger.getLogger("CrowdHammerCoordinator");
	
	private final CoordinatorClusterHandle _cluster;
	private final int _testRunDuration;
	private final int[] _testClientCounts;
	
	public TestRunner(CoordinatorClusterHandle cluster, int testRunDuration, int[] testClientCounts) {
		_cluster = cluster;
		_testRunDuration = testRunDuration;
		_testClientCounts = Objects.requireNonNull(testClientCounts);
	}
	
	@Override
	public void run() {
		// get nodes that will be participating in the test
		ParticipatingNodes participatingNodes = _cluster.getNodeSnapshot();
		
		try {			
			for (int clientCount : _testClientCounts) {	
				setAndWait(CrowdHammerServiceState.INIT_TEST, participatingNodes);
				
				// get worker assignment requests
				List<AssignmentRequest> workerAssignmentReqs = 
						_cluster.getAssignmentRequests(WorkerService.SERVICE_TYPE);
				int globalMaxWorkers = 0;
				Map<String, Integer> workerMaxCountLookup = new HashMap<>(workerAssignmentReqs.size());
				for (AssignmentRequest assignmentReq : workerAssignmentReqs) {
					int maxWorkers = MessageBytesUtil.readInt(assignmentReq.getRequestBytes(), 0);
					globalMaxWorkers += maxWorkers;
					workerMaxCountLookup.put(assignmentReq.getServiceId(), maxWorkers);
				}
				int remainingCount = clientCount;
				List<WorkerAllocation> assignmentAllocations = new ArrayList<>();
				for (Entry<String, Integer> worker : workerMaxCountLookup.entrySet()) {
					int allocatedCount = (int) (((double) worker.getValue() / (double) globalMaxWorkers) * clientCount);
					remainingCount -= allocatedCount;
					assignmentAllocations.add(new WorkerAllocation(UUID.fromString(worker.getKey()), allocatedCount));
				}
				if (remainingCount > 0 && assignmentAllocations.size() > 0) {
					WorkerAllocation prevFirst = assignmentAllocations.get(0);
					assignmentAllocations.remove(0);
					assignmentAllocations.add(0, new WorkerAllocation(prevFirst.getWorkerId(), prevFirst.getAllocation() + remainingCount));
				}
				for (WorkerAllocation allocation : assignmentAllocations) {
					byte[] res = new byte[4];
					MessageBytesUtil.writeInt(res, 0, allocation.getAllocation());
					_cluster.setAssignment(WorkerService.SERVICE_TYPE, allocation.getWorkerId(), res);
				}
				
				// process client handler ID requests
				List<AssignmentRequest> clientHandlerReqs = 
						_cluster.getAssignmentRequests(ClientHandlerService.SERVICE_TYPE);
				int nextId = 0;
				for (AssignmentRequest req : clientHandlerReqs) {
					byte[] res = new byte[4];
					MessageBytesUtil.writeInt(res, 0, nextId++);
					_cluster.setAssignment(ClientHandlerService.SERVICE_TYPE, UUID.fromString(req.getServiceId()), res);
				}
				
				for (CrowdHammerServiceState state : Arrays.asList(SET_UP_TEST, CONNECT_SUT, START_SUT, EXEC_TEST)) {
					setAndWait(state, participatingNodes);
				}
				
				Thread.sleep(TimeUnit.SECONDS.toMillis(_testRunDuration));
				
				for (CrowdHammerServiceState state : Arrays.asList(STOP_SENDING_EVENTS, TEAR_DOWN)) {
					setAndWait(state, participatingNodes);
				}
			}
			setAndWait(SHUTDOWN, participatingNodes);
		} catch (InterruptedException eInterrupted) {
			System.out.println("Test Interrupted");
		}		
	}
	
	private void setAndWait(CrowdHammerServiceState newState, ParticipatingNodes participatingNodes) {
		LOG.info(String.format("Setting state to %s", newState.name()));
		_cluster.setState(newState);
		LOG.info("Waiting for nodes...");
		while (!_cluster.waitForReady(participatingNodes));
		LOG.info("All nodes ready, proceeding...");
	}
	
	private static class WorkerAllocation {
		private final UUID _workerId;
		private final int _allocation;
		
		public WorkerAllocation(final UUID workerId, final int allocation) {
			_workerId = workerId;
			_allocation = allocation;
		}

		public UUID getWorkerId() {
			return _workerId;
		}

		public int getAllocation() {
			return _allocation;
		}
	}
	
}
