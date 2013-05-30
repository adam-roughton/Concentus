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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Objects;

import com.adamroughton.concentus.clienthandler.ClientHandlerService;
import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes;
import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle.AssignmentRequest;
import com.adamroughton.concentus.crowdhammer.metriclistener.MetricListenerService;
import com.adamroughton.concentus.crowdhammer.metriclistener.WorkerInfo;
import com.adamroughton.concentus.crowdhammer.worker.WorkerService;
import com.adamroughton.concentus.messaging.MessageBytesUtil;

import static com.adamroughton.concentus.crowdhammer.CrowdHammerCoordinatorNode.setAndWait;
import static com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState.*;

public final class TestRunner implements Runnable {
	
	private final ParticipatingNodes _participatingNodes;
	private final ClusterCoordinatorHandle _cluster;
	private final int _testRunDuration;
	private final int[] _testClientCounts;
	private final Map<UUID, Long> _workerIdLookup;
	
	public TestRunner(ParticipatingNodes participatingNodes, ClusterCoordinatorHandle cluster, int testRunDuration, int[] testClientCounts, 
			Map<UUID,Long> workerIdLookup) {
		_participatingNodes = Objects.requireNonNull(participatingNodes);
		_cluster = Objects.requireNonNull(cluster);
		_testRunDuration = testRunDuration;
		_testClientCounts = Objects.requireNonNull(testClientCounts);
		_workerIdLookup = workerIdLookup;
	}
	
	@Override
	public void run() {		
		try {			
			for (int clientCount : _testClientCounts) {	
				setAndWait(CrowdHammerServiceState.INIT_TEST, _participatingNodes, _cluster);
				
				List<WorkerInfo> workers = new ArrayList<>();
				
				// get worker assignment requests
				List<AssignmentRequest> workerAssignmentReqs = 
						_cluster.getAssignmentRequests(WorkerService.SERVICE_TYPE);
				for (WorkerAllocation allocation : createWorkerAllocations(clientCount, workerAssignmentReqs)) {
					byte[] res = new byte[4];
					MessageBytesUtil.writeInt(res, 0, allocation.getAllocation());
					_cluster.setAssignment(WorkerService.SERVICE_TYPE, allocation.getWorkerId(), res);
					workers.add(new WorkerInfo(_workerIdLookup.get(allocation.getWorkerId()), allocation.getAllocation()));
				}
				
				// get metric listener client count req				
				List<AssignmentRequest> metricListenerReqs = 
						_cluster.getAssignmentRequests(MetricListenerService.SERVICE_TYPE);
				int cursor = 0;
				for (AssignmentRequest req : metricListenerReqs) {
					byte[] res = new byte[12 * workers.size() + 4];
					MessageBytesUtil.writeInt(res, cursor, workers.size());
					cursor += 4;
					for (int i = 0; i < workers.size(); i++) {
						WorkerInfo workerInfo = workers.get(i);
						MessageBytesUtil.writeLong(res, cursor, workerInfo.getWorkerId());
						cursor += 8;
						MessageBytesUtil.writeInt(res, cursor, workerInfo.getSimClientCount());
						cursor += 4;
					}
					_cluster.setAssignment(MetricListenerService.SERVICE_TYPE, UUID.fromString(req.getServiceId()), res);
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
					setAndWait(state, _participatingNodes, _cluster);
				}
				
				Thread.sleep(TimeUnit.SECONDS.toMillis(_testRunDuration));
				
				for (CrowdHammerServiceState state : Arrays.asList(STOP_SENDING_EVENTS, TEAR_DOWN)) {
					setAndWait(state, _participatingNodes, _cluster);
				}
			}
			setAndWait(SHUTDOWN, _participatingNodes, _cluster);
		} catch (InterruptedException eInterrupted) {
			System.out.println("Test Interrupted");
			try {
				setAndWait(SHUTDOWN, _participatingNodes, _cluster);
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
	
	static List<WorkerAllocation> createWorkerAllocations(int testClientCount, List<AssignmentRequest> workerAssignmentRequests) {
		int workerCount = workerAssignmentRequests.size();
		List<WorkerAllocation> workerAllocations = new ArrayList<>(workerCount);
		int[] workerMaxClientCountLookup = new int[workerCount];
		double[] targetWorkerRatioLookup = new double[workerCount];
		double maxWorkerRatio = 0;
		int globalMaxClients = 0;
		
		for (int i = 0; i < workerCount; i++) {
			AssignmentRequest assignmentReq = workerAssignmentRequests.get(i);
			int workerMaxClientCount = MessageBytesUtil.readInt(assignmentReq.getRequestBytes(), 0);
			workerMaxClientCountLookup[i] = workerMaxClientCount;
			globalMaxClients += workerMaxClientCount;
			workerAllocations.add(i, new WorkerAllocation(UUID.fromString(assignmentReq.getServiceId()), 0));
		}
		
		if (testClientCount > globalMaxClients)
			throw new RuntimeException(String.format("The total available client count %d is less than " +
					"the requested test client count %d.", globalMaxClients, testClientCount));
		
		for (int i = 0; i < workerCount; i++) {
			double workerClientRatio = (double) workerMaxClientCountLookup[i] / (double) globalMaxClients;
			targetWorkerRatioLookup[i] = workerClientRatio;
			if (workerClientRatio > maxWorkerRatio) maxWorkerRatio = workerClientRatio;
		}
		
		WorkerAllocation workerAllocation;
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
				workerAllocations.set(i, new WorkerAllocation(workerAllocation.getWorkerId(), workerAllocation.getAllocation() + allocationChange));
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
			if (workerAllocation.getAllocation() + 1 <= workerMaxClientCountLookup[currentWorkerIndex]) {
				workerAllocations.set(currentWorkerIndex, new WorkerAllocation(workerAllocation.getWorkerId(), workerAllocation.getAllocation() + 1));
				remainingCount--;
			}
			currentWorkerIndex = (currentWorkerIndex + 1) % workerCount;
		}

		return workerAllocations;
	}
	
}
