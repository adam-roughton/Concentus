package com.adamroughton.concentus.crowdhammer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle.AssignmentRequest;
import com.adamroughton.concentus.crowdhammer.TestRunner.WorkerAllocation;
import com.adamroughton.concentus.data.BytesUtil;

import static org.junit.Assert.*;

public class TestTestRunner {

//	private List<AssignmentRequest> _workerAssignmentRequests;
//	
//	@Before
//	public void setUp() {
//		// [1000, 2000, 3000, 4000, 5000, 1000, 2000, 3000, 4000, 5000]; total count = 30000
//		_workerAssignmentRequests = new ArrayList<>();
//		for (int i = 0; i < 10; i++) {
//			int clientCount = 1000 * (i % 5) + 1000;
//			_workerAssignmentRequests.add(createWorkerRequest(i, clientCount));
//		}
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation() {
//		int[] expectedAllocations = new int[] {4, 7, 11, 14, 16, 3, 6, 10, 13, 16 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(100, _workerAssignmentRequests);
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation_OneWorker() {
//		int[] expectedAllocations = new int[] { 100 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(100, Arrays.asList(createWorkerRequest(0, 1000)));
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation_TwoEvenWorkers() {
//		int[] expectedAllocations = new int[] { 50, 50 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(100, Arrays.asList(createWorkerRequest(0, 1000), createWorkerRequest(1, 1000)));
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation_OneClient() {
//		int[] expectedAllocations = new int[] {1, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(1, _workerAssignmentRequests);
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation_SixClients() {
//		int[] expectedAllocations = new int[] {1, 1, 1, 1, 1, 0, 0, 0, 0, 1 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(6, _workerAssignmentRequests);
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation_ZeroClients() {
//		int[] expectedAllocations = new int[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(0, _workerAssignmentRequests);
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test
//	public void testCreateWorkerAllocation_MaxClients() {
//		int[] expectedAllocations = new int[] {1000, 2000, 3000, 4000, 5000, 1000, 2000, 3000, 4000, 5000 };
//		List<WorkerAllocation> workerAllocations = TestRunner.createWorkerAllocations(30000, _workerAssignmentRequests);
//		int[] actualAllocations = new int[workerAllocations.size()];
//		for (int i = 0; i < workerAllocations.size(); i++) {
//			actualAllocations[i] = workerAllocations.get(i).getAllocation();
//		}
//		assertArrayEquals(expectedAllocations, actualAllocations);
//	}
//	
//	@Test(expected=RuntimeException.class)
//	public void testCreateWorkerAllocation_TooManyClients() {
//		TestRunner.createWorkerAllocations(30001, _workerAssignmentRequests);
//	}
//	
//	private AssignmentRequest createWorkerRequest(int workerId, int clientCount) {
//		byte[] clientCountBytes = new byte[4];
//		MessageBytesUtil.writeInt(clientCountBytes, 0, clientCount);
//		return new AssignmentRequest(new UUID(0, workerId).toString(), clientCountBytes);
//	}
	
}
