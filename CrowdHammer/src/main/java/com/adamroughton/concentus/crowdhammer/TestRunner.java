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
package com.adamroughton.concentus.crowdhammer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Objects;

import com.adamroughton.concentus.clienthandler.ClientHandlerService;
import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle.AssignmentRequest;
import com.adamroughton.concentus.cluster.data.WorkerAllocationInfo;
import com.adamroughton.concentus.crowdhammer.worker.WorkerService;
import com.adamroughton.concentus.data.BytesUtil;

import static com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState.*;

public final class TestRunner implements Runnable {
	
	private final CrowdHammerCoordinator _coordinator;
	private final int _testRunDuration;
	private final int[] _testClientCounts;
	
	public TestRunner(CrowdHammerCoordinator coordinator, int testRunDuration, int[] testClientCounts) {
		_coordinator = Objects.requireNonNull(coordinator);
		_testRunDuration = testRunDuration;
		_testClientCounts = Objects.requireNonNull(testClientCounts);
	}
	
	@Override
	public void run() {	
		_coordinator.getClusterHandle().setApplicationClass(TestApplication.class.getName());
		try {			
			for (int clientCount : _testClientCounts) {	
				_coordinator.newTestRun(clientCount, _testRunDuration);
				_coordinator.setAndWait(CrowdHammerServiceState.INIT_TEST);
				ClusterCoordinatorHandle cluster = _coordinator.getClusterHandle();
				
				List<WorkerAllocationInfo> workerAllocations = new ArrayList<>();
				
				// get worker assignment requests
				List<AssignmentRequest> workerAssignmentReqs = 
						cluster.getAssignmentRequests(WorkerService.SERVICE_TYPE);
				List<WorkerAllocationInfo> allocations = createWorkerAllocations(clientCount, workerAssignmentReqs);
				for (WorkerAllocationInfo allocation : allocations) {
					byte[] res = new byte[4];
					BytesUtil.writeInt(res, 0, allocation.getClientAllocation());
					cluster.setAssignment(WorkerService.SERVICE_TYPE, allocation.getWorkerId(), res);
					workerAllocations.add(allocation);
				}
				cluster.setWorkerAllocations(allocations);
				
				// process client handler ID requests
				List<AssignmentRequest> clientHandlerReqs = 
						cluster.getAssignmentRequests(ClientHandlerService.SERVICE_TYPE);
				int nextId = 0;
				for (AssignmentRequest req : clientHandlerReqs) {
					byte[] res = new byte[4];
					BytesUtil.writeInt(res, 0, nextId++);
					cluster.setAssignment(ClientHandlerService.SERVICE_TYPE, UUID.fromString(req.getServiceId()), res);
				}
				
				for (CrowdHammerServiceState state : Arrays.asList(SET_UP_TEST, CONNECT_SUT, START_SUT, EXEC_TEST)) {
					_coordinator.setAndWait(state);
				}
				
				//Thread.sleep(TimeUnit.SECONDS.toMillis(_testRunDuration));
				Thread.sleep(TimeUnit.DAYS.toMillis(30));
				
				
				for (CrowdHammerServiceState state : Arrays.asList(STOP_SENDING_EVENTS, TEAR_DOWN)) {
					_coordinator.setAndWait(state);
				}
			}
			_coordinator.setAndWait(SHUTDOWN);
		} catch (InterruptedException eInterrupted) {
			System.out.println("Test Interrupted");
			try {
				_coordinator.setAndWait(SHUTDOWN);
			} catch (InterruptedException e) {
			}
		}		
	}
	
	static class WorkerAllocation {
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
	
	static List<WorkerAllocationInfo> createWorkerAllocations(int testClientCount, List<AssignmentRequest> workerAssignmentRequests) {
		int workerCount = workerAssignmentRequests.size();
		List<WorkerAllocationInfo> workerAllocations = new ArrayList<>(workerCount);
		int[] workerMaxClientCountLookup = new int[workerCount];
		double[] targetWorkerRatioLookup = new double[workerCount];
		double maxWorkerRatio = 0;
		int globalMaxClients = 0;
		
		for (int i = 0; i < workerCount; i++) {
			AssignmentRequest assignmentReq = workerAssignmentRequests.get(i);
			int workerMaxClientCount = BytesUtil.readInt(assignmentReq.getRequestBytes(), 0);
			workerMaxClientCountLookup[i] = workerMaxClientCount;
			globalMaxClients += workerMaxClientCount;
			workerAllocations.add(i, new WorkerAllocationInfo(UUID.fromString(assignmentReq.getServiceId()), 0));
		}
		
		if (testClientCount > globalMaxClients)
			throw new RuntimeException(String.format("The total available client count %d is less than " +
					"the requested test client count %d.", globalMaxClients, testClientCount));
		
		for (int i = 0; i < workerCount; i++) {
			double workerClientRatio = (double) workerMaxClientCountLookup[i] / (double) globalMaxClients;
			targetWorkerRatioLookup[i] = workerClientRatio;
			if (workerClientRatio > maxWorkerRatio) maxWorkerRatio = workerClientRatio;
		}
		
		WorkerAllocationInfo workerAllocation;
		int remainingCount = testClientCount;
		while (remainingCount * maxWorkerRatio >= 1) {
			/* we will be able to allocate at least one on this run by applying
			 * the target ratios, so continue
			 */
			long runAllocationCount = remainingCount;
			long amountAllocatedOnRun = 0;
			for (int i = 0; i < workerCount; i++) {
				workerAllocation = workerAllocations.get(i);
				int allocationChange = (int) (runAllocationCount * targetWorkerRatioLookup[i]);
				workerAllocations.set(i, new WorkerAllocationInfo(workerAllocation.getWorkerId(), workerAllocation.getClientAllocation() + allocationChange));
				amountAllocatedOnRun += allocationChange;
			}
			remainingCount -= amountAllocatedOnRun;
		}
		
		int currentWorkerIndex = 0;
		while (remainingCount > 0) {
			/* all target ratios will give 0 for the remaining amount,
			 * so resort to adding one at a time up to the maximum
			 * amount for each worker
			 */
			workerAllocation = workerAllocations.get(currentWorkerIndex);
			if (workerAllocation.getClientAllocation() + 1 <= workerMaxClientCountLookup[currentWorkerIndex]) {
				workerAllocations.set(currentWorkerIndex, new WorkerAllocationInfo(workerAllocation.getWorkerId(), workerAllocation.getClientAllocation() + 1));
				remainingCount--;
			}
			currentWorkerIndex = (currentWorkerIndex + 1) % workerCount;
		}

		return workerAllocations;
	}
	
}
