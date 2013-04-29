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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
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
					UUID workerId = UUID.fromString(worker.getKey());
					assignmentAllocations.add(new WorkerAllocation(workerId, allocatedCount));
					workers.add(new WorkerInfo(_workerIdLookup.get(workerId), allocatedCount));
				}
				int index = 0;
				while(remainingCount > 0 && index < assignmentAllocations.size()) {
					WorkerAllocation prevAllocation = assignmentAllocations.get(index);
					assignmentAllocations.remove(index);
					workers.remove(index);
					int newAllocation = prevAllocation.getAllocation() + 1;
					assignmentAllocations.add(index, new WorkerAllocation(prevAllocation.getWorkerId(), newAllocation));
					workers.add(new WorkerInfo(_workerIdLookup.get(prevAllocation.getWorkerId()), newAllocation));
					index = (index + 1) % assignmentAllocations.size();
				}
				for (WorkerAllocation allocation : assignmentAllocations) {
					byte[] res = new byte[4];
					MessageBytesUtil.writeInt(res, 0, allocation.getAllocation());
					_cluster.setAssignment(WorkerService.SERVICE_TYPE, allocation.getWorkerId(), res);
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
