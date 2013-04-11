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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.cluster.ClusterPath;
import com.adamroughton.concentus.cluster.ClusterState;
import com.adamroughton.concentus.cluster.ClusterUtil;
import com.adamroughton.concentus.cluster.ExceptionCallback;
import com.adamroughton.concentus.cluster.TestClusterBase;
import com.adamroughton.concentus.cluster.TestState1;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes;
import com.adamroughton.concentus.cluster.coordinator.Cluster.AssignmentRequest;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes.ParticipatingNodesLatch;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.google.common.collect.Sets;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;

import static org.junit.Assert.*;

public class TestClusterCoordinator extends TestClusterBase {

	private final static UUID COORDINATOR_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
	
	private CoordinatorClusterHandle _clusterCoordinator;
	private CuratorFramework _client;
	private ExceptionCallback _exCallback;
	
	@Before
	public void setUp() {
		_client = CuratorFrameworkFactory.newClient(getZooKeeperAddress(), new ExponentialBackoffRetry(1000, 3));
		_client.start();
		_exCallback = new ExceptionCallback();
		_clusterCoordinator = new CoordinatorClusterHandle(getZooKeeperAddress(), ROOT, COORDINATOR_ID, _exCallback);
	}
	
	@After
	public void tearDown() throws Exception {
		_clusterCoordinator.close();
		_client.close();
	}
	
	private static class ServicePacket {
		public final String _serviceType;
		public final byte[] _data;
		public final UUID _id;
		
		public ServicePacket(final String serviceType, final byte[] data, final UUID id) {
			_serviceType = serviceType;
			_data = data;
			_id = id;
		}
		
		public String getServiceType() {
			return _serviceType;
		}
		
		public byte[] getData() {
			byte[] copy = new byte[_data.length];
			System.arraycopy(_data, 0, copy, 0, copy.length);
			return copy;
		}
		
		public UUID getId() {
			return _id;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((_id == null) ? 0 : _id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ServicePacket other = (ServicePacket) obj;
			if (_id == null) {
				if (other._id != null)
					return false;
			} else if (!_id.equals(other._id))
				return false;
			return true;
		}
	    
	}
	
	private ServicePacket fakeRequestAssignment(final String serviceType, int index) throws Exception {
		CuratorFramework testClient = getTestClient();
		
		byte[] reqData = new byte[4];
		MessageBytesUtil.writeInt(reqData, 0, index * 31);
		
		UUID serviceId = new UUID(23 * index, 79 * index);
		
		String reqAssignmentBasePath = ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), serviceType);
		String reqAssignmentPath = ZKPaths.makePath(reqAssignmentBasePath, serviceId.toString());
		if (testClient.checkExists().forPath(reqAssignmentBasePath) == null) {
			testClient.create().creatingParentsIfNeeded().forPath(reqAssignmentBasePath);
		}
		testClient.create().withMode(CreateMode.EPHEMERAL).forPath(reqAssignmentPath, reqData);
		
		return new ServicePacket(serviceType, reqData, serviceId);
	}
	
	@Test
	public void getAssignmentRequestServiceTypesOneType() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		fakeRequestAssignment(serviceType, 0);
		fakeRequestAssignment(serviceType, 1);
		
		List<String> serviceTypes = _clusterCoordinator.getAssignmentRequestServiceTypes();
		_exCallback.throwAnyExceptions();
		
		assertEquals(1, serviceTypes.size());
		assertEquals(serviceType, serviceTypes.get(0));
	}
	
	@Test
	public void getAssignmentRequestServiceTypesMultipleTypes() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType1 = "TEST1";
		fakeRequestAssignment(serviceType1, 0);
		fakeRequestAssignment(serviceType1, 1);
		
		String serviceType2 = "TEST2";
		fakeRequestAssignment(serviceType2, 0);
		
		String serviceType3 = "TEST3";
		fakeRequestAssignment(serviceType3, 0);
		fakeRequestAssignment(serviceType3, 1);
		fakeRequestAssignment(serviceType3, 2);
		
		Set<String> expectedTypes = Sets.newHashSet(serviceType1, serviceType2, serviceType3);
		
		List<String> serviceTypes = _clusterCoordinator.getAssignmentRequestServiceTypes();
		_exCallback.throwAnyExceptions();
		
		for (String serviceType : serviceTypes) {
			expectedTypes.remove(serviceType);
		}
		assertEquals("Not all service types were returned", 0, expectedTypes.size());
	}
	
	@Test
	public void getAssignmentRequestServiceTypesNoTypes() throws Throwable {	
		_clusterCoordinator.start();
		
		List<String> serviceTypes = _clusterCoordinator.getAssignmentRequestServiceTypes();
		_exCallback.throwAnyExceptions();
		
		assertEquals(0, serviceTypes.size());
	}
	
	@Test
	public void getAssignmentRequestsOneRequest() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		ServicePacket info = fakeRequestAssignment(serviceType, 0);
		
		List<AssignmentRequest> assignmentReqs = _clusterCoordinator.getAssignmentRequests(serviceType);
		_exCallback.throwAnyExceptions();
		
		assertEquals(1, assignmentReqs.size());
		assertEquals(info.getId().toString(), assignmentReqs.get(0).getServiceId());
		assertArrayEquals(info.getData(), assignmentReqs.get(0).getRequestBytes());
	}
	
	@Test
	public void getAssignmentRequestsMultipleRequests() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		Map<String, ServicePacket> requestLookup = new HashMap<>();
		for (int i = 0; i < 7; i++) {
			ServicePacket info = fakeRequestAssignment(serviceType, i);
			requestLookup.put(info.getId().toString(), info);
		}	
		
		List<AssignmentRequest> assignmentReqs = _clusterCoordinator.getAssignmentRequests(serviceType);
		_exCallback.throwAnyExceptions();
		
		for (AssignmentRequest req : assignmentReqs) {
			String serviceId = req.getServiceId();
			ServicePacket info = requestLookup.get(serviceId);
			assertArrayEquals(info.getData(), req.getRequestBytes());
			requestLookup.remove(serviceId);
		}
		assertEquals("Not all assignment requests were returned", 0, requestLookup.size());
	}
	
	@Test
	public void getAssignmentRequestsMultipleRequestsMultipleServiceTypes() throws Throwable {
		_clusterCoordinator.start();
		
		Map<String, ServicePacket> requestLookup = new HashMap<>();
		
		String serviceType1 = "TEST1";
		for (int i = 0; i < 7; i++) {
			ServicePacket info = fakeRequestAssignment(serviceType1, requestLookup.size());
			requestLookup.put(info.getId().toString(), info);
		}
		
		String serviceType2 = "TEST2";
		for (int i = 0; i < 3; i++) {
			ServicePacket info = fakeRequestAssignment(serviceType2, requestLookup.size());
			requestLookup.put(info.getId().toString(), info);
		}
		
		String serviceType3 = "TEST3";
		for (int i = 0; i < 10; i++) {
			ServicePacket info = fakeRequestAssignment(serviceType3, requestLookup.size());
			requestLookup.put(info.getId().toString(), info);
		}
		
		for (String serviceType : Arrays.asList(serviceType1, serviceType2, serviceType3)) {
			List<AssignmentRequest> assignmentReqs = _clusterCoordinator.getAssignmentRequests(serviceType);
			_exCallback.throwAnyExceptions();
			
			for (AssignmentRequest req : assignmentReqs) {
				ServicePacket info = requestLookup.get(req.getServiceId());
				assertArrayEquals(info.getData(), req.getRequestBytes());
				requestLookup.remove(req.getServiceId());
			}
		}
		assertEquals("Not all assignment requests were returned", 0, requestLookup.size());
	}
	
	@Test
	public void getAssignmentRequestsNoRequests() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		getTestClient().create().forPath(ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), serviceType));
		
		List<AssignmentRequest> assignmentReqs = _clusterCoordinator.getAssignmentRequests(serviceType);
		_exCallback.throwAnyExceptions();
		
		assertEquals(0, assignmentReqs.size());
	}
	
	@Test
	public void getAssignmentRequestsNoServiceType() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		
		List<AssignmentRequest> assignmentReqs = _clusterCoordinator.getAssignmentRequests(serviceType);
		_exCallback.throwAnyExceptions();
		
		assertEquals(0, assignmentReqs.size());
	}
	
	private ServicePacket createFakeRes(String serviceType, int index, UUID id) {
		byte[] resData = new byte[4];
		MessageBytesUtil.writeInt(resData, 0, 33 * index);
		return new ServicePacket(serviceType, resData, id);
	}
	
	private ServicePacket createFakeRes(String serviceType, int index) {
		UUID id = new UUID(7 * index, 13 * index);
		return createFakeRes(serviceType, index, id);
	}
	
	@Test
	public void setAssignmentFirstCallOnStateType() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		ServicePacket info = createFakeRes(serviceType, 0);
		_clusterCoordinator.setAssignment(info.getServiceType(), info.getId(), info.getData());
		_exCallback.throwAnyExceptions();
		
		String resRoot = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), serviceType);
		String serviceResPath = ZKPaths.makePath(resRoot, info.getId().toString());
		
		assertTrue("Assignment was not created", getTestClient().checkExists().forPath(serviceResPath) != null);
		assertArrayEquals(info.getData(), getTestClient().getData().forPath(serviceResPath));
	}
	
	@Test
	public void setAssignmentMultipleCallsOnStateType() throws Throwable {
		_clusterCoordinator.start();
		
		String serviceType = "TEST";
		for (int i = 0; i < 4; i++) {
			ServicePacket info = createFakeRes(serviceType, i);
			_clusterCoordinator.setAssignment(info.getServiceType(), info.getId(), info.getData());
			_exCallback.throwAnyExceptions();
			
			String resRoot = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), serviceType);
			String serviceResPath = ZKPaths.makePath(resRoot, info.getId().toString());
			
			assertTrue("Assignment was not created", getTestClient().checkExists().forPath(serviceResPath) != null);
			assertArrayEquals(info.getData(), getTestClient().getData().forPath(serviceResPath));
		}
	}
	
	@Test
	public void setAssignmentMultipleStateTypes() throws Throwable {
		_clusterCoordinator.start();
		
		int baseIndex = 0;
		for (String serviceType : Arrays.asList("TEST1", "TEST2", "TEST3")) {
			for (int i = 0; i < 4; i++) {
				ServicePacket info = createFakeRes(serviceType, i + baseIndex);
				_clusterCoordinator.setAssignment(info.getServiceType(), info.getId(), info.getData());
				_exCallback.throwAnyExceptions();
				
				String resRoot = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), serviceType);
				String serviceResPath = ZKPaths.makePath(resRoot, info.getId().toString());
				
				assertTrue("Assignment was not created", getTestClient().checkExists().forPath(serviceResPath) != null);
				assertArrayEquals(info.getData(), getTestClient().getData().forPath(serviceResPath));
			}
			baseIndex++;
		}
	}
	
	@Test
	public void setStateInitial() throws Throwable {
		_clusterCoordinator.start();
		
		_clusterCoordinator.setState(TestState1.ONE);
		_exCallback.throwAnyExceptions();
		
		assertTrue("State Exists", getTestClient().checkExists().forPath(ClusterPath.READY.getPath(ROOT)) != null);
		
		ClusterState clusterState = ClusterState.fromBytes(
				getTestClient().getData().forPath(ClusterPath.STATE.getPath(ROOT)));
		assertEquals(TestState1.ONE.code(), clusterState.getStateCode());
		assertEquals(TestState1.ONE.domain(), clusterState.getStateDomain());
	}
	
	// test order of operations
	
	@Test
	public void setStateSubsequent() throws Throwable {
		_clusterCoordinator.start();
		
		// create previous state
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.STATE.getPath(ROOT));
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.ASSIGN_REQ.getPath(ROOT));
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.ASSIGN_RES.getPath(ROOT));
		 
		ClusterState prevState = new ClusterState(TestState1.ONE.domain(), TestState1.ONE.code());
		getTestClient().setData().forPath(ClusterPath.STATE.getPath(ROOT), ClusterState.toBytes(prevState));
		
		// create some services and service assignment requests
		int serviceIndex = 0;
		List<ServicePacket> services = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			services.add(fakeRequestAssignment("TEST1", serviceIndex++));
		}
		for (int i = 0; i < 3; i++) {
			services.add(fakeRequestAssignment("TEST2", serviceIndex++));
		}
		
		// create some ready flags and assignment responses
		for (ServicePacket service : services) {
			// ready flags
			String serviceReadyPath = ZKPaths.makePath(ClusterPath.READY.getPath(ROOT), service.getId().toString());
			getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(serviceReadyPath, null);
			
			// fake assignment response
			ServicePacket resPacket = createFakeRes(service.getServiceType(), serviceIndex++, service.getId());
			String resPath = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), resPacket.getId().toString());
			getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(resPath, resPacket.getData());
		}
		
		_clusterCoordinator.setState(TestState1.TWO);
		_exCallback.throwAnyExceptions();
		
		assertTrue("State Exists", getTestClient().checkExists().forPath(ClusterPath.READY.getPath(ROOT)) != null);
		
		ClusterState clusterState = ClusterState.fromBytes(
				getTestClient().getData().forPath(ClusterPath.STATE.getPath(ROOT)));
		assertEquals(TestState1.TWO.code(), clusterState.getStateCode());
		assertEquals(TestState1.TWO.domain(), clusterState.getStateDomain());
		
		assertTrue("Ready path was deleted", getTestClient().checkExists().forPath(ClusterPath.READY.getPath(ROOT)) != null);
		assertTrue("Assignment request path was deleted", getTestClient().checkExists().forPath(ClusterPath.ASSIGN_REQ.getPath(ROOT)) != null);
		assertTrue("Assignment response path was deleted", getTestClient().checkExists().forPath(ClusterPath.ASSIGN_RES.getPath(ROOT)) != null);
		
		// assert that ready signals have been deleted
		assertEquals(0, getTestClient().getChildren().forPath(ClusterPath.READY.getPath(ROOT)).size());
		// assert that previous assignment requests have been deleted
		assertEquals(0, getTestClient().getChildren().forPath(ClusterPath.ASSIGN_REQ.getPath(ROOT)).size());
		// assert that assignment responses are still present
		assertEquals(services.size(), getTestClient().getChildren().forPath(ClusterPath.ASSIGN_RES.getPath(ROOT)).size());
	}
	
	// helper classes for event order
//	
//	private static class EventOrderCapturer implements CuratorListener {
//
//		private final List<CapturedEvent> _capturedEvents = new ArrayList<>();
//		
//		@Override
//		public synchronized void eventReceived(CuratorFramework client, CuratorEvent event)
//				throws Exception {
//			_capturedEvents.add(new CapturedEvent(event.getType(), event.getPath()));
//		}
//		
//		public synchronized List<CapturedEvent> getCapturedEvents() {
//			return new ArrayList<>(_capturedEvents);
//		}
//		
//	}
//	
//	private static class CapturedEvent {
//		
//		public final CuratorEventType eventType;
//		public final String path;
//		
//		public CapturedEvent(final CuratorEventType eventType, final String path) {
//			this.eventType = eventType;
//			this.path = path;
//		}
//		
//	}
//	
//	// end of event order helper classes
//	
//	@Test
//	public void setStateOperationsInOrder() throws Throwable {
//		_clusterCoordinator.start();
//		
//		// create previous state
//		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.STATE.getPath(ROOT));
//		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
//		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.ASSIGN_REQ.getPath(ROOT));
//		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.ASSIGN_RES.getPath(ROOT));
//		 
//		ClusterState prevState = new ClusterState(TestState1.ONE.domain(), TestState1.ONE.code());
//		getTestClient().setData().forPath(ClusterPath.STATE.getPath(ROOT), ClusterState.toBytes(prevState));
//		
//		// create some services and service assignment requests
//		int serviceIndex = 0;
//		List<ServicePacket> services = new ArrayList<>();
//		for (int i = 0; i < 6; i++) {
//			services.add(fakeRequestAssignment("TEST1", serviceIndex++));
//		}
//		for (int i = 0; i < 3; i++) {
//			services.add(fakeRequestAssignment("TEST2", serviceIndex++));
//		}
//		
//		// create some ready flags and assignment responses
//		for (ServicePacket service : services) {
//			String serviceReadyPath = ZKPaths.makePath(ClusterPath.READY.getPath(ROOT), service.getId().toString());
//			getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(serviceReadyPath, null);
//			createFakeRes(service.getServiceType(), serviceIndex++, service.getId());
//		}
//		
//		EventOrderCapturer eventOrder = new EventOrderCapturer();
//		getTestClient().getCuratorListenable().addListener(eventOrder); // watched not called on client
//		_clusterCoordinator.setState(TestState1.TWO);
//		_exCallback.throwAnyExceptions();
//		
//		boolean startAsserting = false;
//		for (CapturedEvent event : eventOrder.getCapturedEvents()) {
//			if (event.path.equals(ClusterPath.STATE.getPath(ROOT))) {
//				startAsserting = true;
//			}
//			if (startAsserting) {
//				assertFalse("Ready state was changed after setting state", event.path.startsWith(ClusterPath.READY.getPath(ROOT)));
//				assertFalse("Assignment requests were changed after setting state", event.path.startsWith(ClusterPath.ASSIGN_REQ.getPath(ROOT)));
//				assertFalse("Assignment responses were changed after setting state", event.path.startsWith(ClusterPath.ASSIGN_RES.getPath(ROOT)));
//			}
//		}
//	}
	
	private UUID createFakeReady(int index, UUID id) throws Exception {
		String serviceReadyPath = ZKPaths.makePath(ClusterPath.READY.getPath(ROOT), id.toString());
		
		getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(serviceReadyPath, null);
		
		return id;
	}
	
	private UUID createFakeReady(int index) throws Exception {
		UUID id = new UUID(7 * index, 13 * index);
		return createFakeReady(index, id);
	}
	
	@Test
	public void getNodeSnapshotOneService() throws Throwable {
		_clusterCoordinator.start();
		
		UUID serviceId = createFakeReady(0);
		
		ParticipatingNodes participatingNodes = _clusterCoordinator.getNodeSnapshot();
		_exCallback.throwAnyExceptions();
		
		assertEquals(1, participatingNodes.getCount());
		ParticipatingNodesLatch latch = participatingNodes.createNodesLatch();
		latch.accountFor(serviceId);
		assertTrue(latch.isDone());
	}
	
	@Test
	public void getNodeSnapshotManyServices() throws Throwable {
		_clusterCoordinator.start();
		
		List<UUID> services = new ArrayList<>();
		for (int i = 0; i < 12; i++) {
			services.add(createFakeReady(i));
		}
		
		ParticipatingNodes participatingNodes = _clusterCoordinator.getNodeSnapshot();
		_exCallback.throwAnyExceptions();
		
		assertEquals(services.size(), participatingNodes.getCount());
		ParticipatingNodesLatch latch = participatingNodes.createNodesLatch();
		
		for (UUID serviceId : services) {
			latch.accountFor(serviceId);
		}
		assertTrue(latch.isDone());
	}
	
	@Test
	public void getNodeSnapshotNoServices() throws Throwable {
		_clusterCoordinator.start();
		
		ParticipatingNodes participatingNodes = _clusterCoordinator.getNodeSnapshot();
		_exCallback.throwAnyExceptions();
		
		assertEquals(0, participatingNodes.getCount());
		ParticipatingNodesLatch latch = participatingNodes.createNodesLatch();
		
		assertTrue(latch.isDone());
	}
	
	@Test(timeout=5000)
	public void waitForReadyOneService() throws Throwable {
		_clusterCoordinator.start();
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
		
		UUID serviceId = new UUID(23, 37);
		final ParticipatingNodes participatingNodes = ParticipatingNodes.create().add(serviceId);
		
		final CoordinatorClusterHandle clusterCoordinator = _clusterCoordinator;
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> waitingTaskFuture = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				return clusterCoordinator.waitForReady(participatingNodes);
			}
		});
		
		createFakeReady(0, serviceId);
		assertTrue(waitingTaskFuture.get());
		_exCallback.throwAnyExceptions();
	}
	
	@Test(timeout=5000)
	public void waitForReadyManyServices() throws Throwable {
		_clusterCoordinator.start();
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
		
		ParticipatingNodes participatingNodes = ParticipatingNodes.create();
		List<UUID> serviceIds = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			UUID serviceId = new UUID(13 * i, 7 * i);
			serviceIds.add(serviceId);
			participatingNodes = participatingNodes.add(serviceId);
		}
		
		final CoordinatorClusterHandle clusterCoordinator = _clusterCoordinator;
		final ParticipatingNodes partNodes = participatingNodes;
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> waitingTaskFuture = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				return clusterCoordinator.waitForReady(partNodes);
			}
		});
		
		int serviceIndex = 0;
		for (UUID serviceId : serviceIds) {
			createFakeReady(serviceIndex++, serviceId);
		}
		
		assertTrue(waitingTaskFuture.get());
		_exCallback.throwAnyExceptions();
	}
	
	@Test(timeout=5000)
	public void waitForReadyNoServices() throws Throwable {
		_clusterCoordinator.start();
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
		
		final ParticipatingNodes participatingNodes = ParticipatingNodes.create();
		
		final CoordinatorClusterHandle clusterCoordinator = _clusterCoordinator;
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> waitingTaskFuture = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				return clusterCoordinator.waitForReady(participatingNodes);
			}
		});
		
		waitingTaskFuture.get();
		_exCallback.throwAnyExceptions();
	}
	
	@Test(timeout=1000)
	public void waitForReadyBeforeTimeOut() throws Throwable {
		_clusterCoordinator.start();
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
		
		ParticipatingNodes participatingNodes = ParticipatingNodes.create();
		List<UUID> serviceIds = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			UUID serviceId = new UUID(13 * i, 7 * i);
			serviceIds.add(serviceId);
			participatingNodes = participatingNodes.add(serviceId);
		}
		
		final CoordinatorClusterHandle clusterCoordinator = _clusterCoordinator;
		final ParticipatingNodes partNodes = participatingNodes;
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> waitingTaskFuture = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				return clusterCoordinator.waitForReady(partNodes, 500, TimeUnit.MILLISECONDS);
			}
		});
		
		int serviceIndex = 0;
		for (UUID serviceId : serviceIds) {
			createFakeReady(serviceIndex++, serviceId);
		}
		
		assertTrue("waitForReady indicated it timed out", waitingTaskFuture.get());
		_exCallback.throwAnyExceptions();
	}
	
	@Test(timeout=5000)
	public void waitForReadyTimeOutExpired() throws Throwable {
		_clusterCoordinator.start();
		ClusterUtil.ensurePathCreated(getTestClient(), ClusterPath.READY.getPath(ROOT));
		
		ParticipatingNodes participatingNodes = ParticipatingNodes.create();
		List<UUID> serviceIds = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			UUID serviceId = new UUID(13 * i, 7 * i);
			serviceIds.add(serviceId);
			participatingNodes = participatingNodes.add(serviceId);
		}
		
		final CoordinatorClusterHandle clusterCoordinator = _clusterCoordinator;
		final ParticipatingNodes partNodes = participatingNodes;
		ExecutorService executor = Executors.newCachedThreadPool();
		Future<Boolean> waitingTaskFuture = executor.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() {
				return clusterCoordinator.waitForReady(partNodes, 500, TimeUnit.MILLISECONDS);
			}
		});
		
		Thread.sleep(600);
		
		int serviceIndex = 0;
		for (UUID serviceId : serviceIds) {
			createFakeReady(serviceIndex++, serviceId);
		}
		
		assertFalse("waitForReady should have timed out", waitingTaskFuture.get());
		_exCallback.throwAnyExceptions();
	}
	
}
