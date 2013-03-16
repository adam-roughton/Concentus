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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.consentus.cluster.ClusterPath;
import com.adamroughton.consentus.cluster.ClusterState;
import com.adamroughton.consentus.cluster.ExceptionCallback;
import com.adamroughton.consentus.cluster.TestClusterBase;
import com.adamroughton.consentus.cluster.TestState1;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;

import static org.junit.Assert.*;

public class TestClusterWorker extends TestClusterBase {

	private final static UUID WORKER_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
	
	private ExecutorService _executor;
	private ExceptionCallback _exCallback;
	private ClusterListenerStateCapturer<TestState1> _stateChangeCapturer;
	private ClusterWorker _clusterWorker;
	
	@Before
	public void setUp() {
		_executor = Executors.newCachedThreadPool();
		_exCallback = new ExceptionCallback();
		_stateChangeCapturer = new ClusterListenerStateCapturer<>(TestState1.class);
		
		_clusterWorker = new ClusterWorker(getZooKeeperAddress(), ROOT, WORKER_ID, _stateChangeCapturer, _executor, _exCallback);
	}
	
	@After
	public void tearDown() throws Exception {
		_clusterWorker.close();
	}
	
	@Test
	public void initStateActedOn() throws Throwable {
		TestState1 initState = TestState1.ONE;
		ClusterState initStateData = new ClusterState(initState.domain(), initState.code());
		getTestClient().create()
			.creatingParentsIfNeeded()
			.forPath(ClusterPath.STATE.getPath(ROOT), ClusterState.toBytes(initStateData));
		
		_clusterWorker.start();
		
		_stateChangeCapturer.getValueCollector().waitForCount(1, 1000, TimeUnit.MILLISECONDS);
		
		_exCallback.throwAnyExceptions();
		
		List<TestState1> stateChanges = _stateChangeCapturer.getValueCollector().getValues();
		assertEquals(1, stateChanges.size());
		assertEquals(TestState1.ONE, stateChanges.get(0));
	}
	
	@Test
	public void testMyIdString() throws Exception {
		assertEquals(WORKER_ID.toString(), _clusterWorker.getMyIdString());
	}
	
	@Test
	public void signalReady() throws Throwable {
		_clusterWorker.start();
		_clusterWorker.signalReady();
		
		String readyPath = ZKPaths.makePath(ClusterPath.READY.getPath(ROOT), 
				_clusterWorker.getMyIdString());
		
		_exCallback.throwAnyExceptions();
		
		CuratorFramework testClient = getTestClient();
		assertTrue("The ready signal was not created", testClient.checkExists().forPath(readyPath) != null);
	}
	
	@Test
	public void signalReadyTwice() throws Throwable {
		_clusterWorker.start();
		_clusterWorker.signalReady();
		_clusterWorker.signalReady();
		
		String readyPath = ZKPaths.makePath(ClusterPath.READY.getPath(ROOT), 
				_clusterWorker.getMyIdString());
		
		_exCallback.throwAnyExceptions();
		
		CuratorFramework testClient = getTestClient();
		assertTrue("The ready signal was not created", testClient.checkExists().forPath(readyPath) != null);
	}
	
	@Test
	public void registerService() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		String address = "tcp://127.0.0.1";
		
		_clusterWorker.registerService(serviceType, address);
		_exCallback.throwAnyExceptions();
		
		String serviceBasePath = ZKPaths.makePath(ClusterPath.SERVICES.getPath(ROOT), serviceType);
		String servicePath = ZKPaths.makePath(serviceBasePath, _clusterWorker.getMyIdString());
		
		CuratorFramework testClient = getTestClient();
		assertTrue("The service registration was not created", testClient.checkExists().forPath(servicePath) != null);
		assertEquals(address, new String(testClient.getData().forPath(servicePath)));
	}
	
	@Test
	public void registerServiceTwiceOver() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		String address1 = "tcp://127.0.0.1";
		String address2 = "tcp://235.453.546.23";
		
		_clusterWorker.registerService(serviceType, address1);
		_clusterWorker.registerService(serviceType, address2);
		_exCallback.throwAnyExceptions();
		
		String serviceBasePath = ZKPaths.makePath(ClusterPath.SERVICES.getPath(ROOT), serviceType);
		String servicePath = ZKPaths.makePath(serviceBasePath, _clusterWorker.getMyIdString());
		
		CuratorFramework testClient = getTestClient();
		assertTrue("The service registration was not created", testClient.checkExists().forPath(servicePath) != null);
		assertEquals(address2, new String(testClient.getData().forPath(servicePath)));
	}

	// Helpers for getService calls --
	
	private class ServiceInfo {
		public final String serviceType;
		public final String address;
		public final UUID id;
		
		public ServiceInfo (final String serviceType, final String address, final UUID id) {
			this.serviceType = serviceType;
			this.address = address;
			this.id = id;
		}
	}
	
	private ServiceInfo putRandomService(final String serviceType, int index) throws Exception {
		CuratorFramework testClient = getTestClient();
		
		String address = "tcp://34.893.153." + index;
		UUID serviceId = new UUID(23 * index, 79 * index);
		
		String serviceBasePath = ZKPaths.makePath(ClusterPath.SERVICES.getPath(ROOT), serviceType);
		String servicePath = ZKPaths.makePath(serviceBasePath, serviceId.toString());
		
		testClient.create().creatingParentsIfNeeded().forPath(servicePath, address.getBytes());
		
		return new ServiceInfo(serviceType, address, serviceId);
	}
	
	// end of helpers --
	
	@Test
	public void getServiceAtRandomOneService() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		ServiceInfo info = putRandomService(serviceType, 0);
		
		String discoveredService = _clusterWorker.getServiceAtRandom(serviceType);
		_exCallback.throwAnyExceptions();
		assertEquals(info.address, discoveredService);
	}
	
	@Test
	public void getServiceAtRandomManyServices() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		List<ServiceInfo> possibleServices = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			possibleServices.add(putRandomService(serviceType, i));
		}
		
		String discoveredService = _clusterWorker.getServiceAtRandom(serviceType);
		_exCallback.throwAnyExceptions();
		
		boolean matchFound = false;
		for (ServiceInfo possibleService : possibleServices) {
			if (possibleService.address.equals(discoveredService)) {
				matchFound = true;
				break;
			}
		}
		assertTrue("Discovered service string did not match any of the available address strings", matchFound);
	}
	
	@Test
	public void getServiceAtRandomNoServices() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		
		String discoveredService = _clusterWorker.getServiceAtRandom(serviceType);
		_exCallback.throwAnyExceptions();
		assertEquals(null, discoveredService);
	}
	
	@Test
	public void getAllServicesOneService() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		Set<String> possibleServices = new HashSet<>();
		possibleServices.add(putRandomService(serviceType, 0).address);
		
		String[] discoveredServices = _clusterWorker.getAllServices(serviceType);
		_exCallback.throwAnyExceptions();
		
		for (String discoveredService : discoveredServices) {
			possibleServices.remove(discoveredService);
		}
		assertEquals("Not all services were discovered", 0, possibleServices.size());
	}
	
	@Test
	public void getAllServicesManyServices() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		Set<String> possibleServices = new HashSet<>();
		for (int i = 0; i < 6; i++) {
			possibleServices.add(putRandomService(serviceType, i).address);
		}
		
		String[] discoveredServices = _clusterWorker.getAllServices(serviceType);
		_exCallback.throwAnyExceptions();
		
		for (String discoveredService : discoveredServices) {
			possibleServices.remove(discoveredService);
		}
		assertEquals("Not all services were discovered", 0, possibleServices.size());
	}
	
	@Test
	public void getAllServicesManyServicesPlusOthersOfDifferentType() throws Throwable {
		_clusterWorker.start();
		
		String serviceType1 = "TEST1";
		Set<String> possibleServices = new HashSet<>();
		for (int i = 0; i < 6; i++) {
			possibleServices.add(putRandomService(serviceType1, i).address);
		}
		
		String serviceType2 = "TEST2";
		for (int i = 0; i < 7; i++) {
			putRandomService(serviceType2, i);
		}
		
		String[] discoveredServices = _clusterWorker.getAllServices(serviceType1);
		_exCallback.throwAnyExceptions();
		
		for (String discoveredService : discoveredServices) {
			possibleServices.remove(discoveredService);
		}
		assertEquals("Not all services were discovered", 0, possibleServices.size());
	}
	
	@Test
	public void getAllServicesNoServices() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		
		String[] discoveredServices = _clusterWorker.getAllServices(serviceType);
		_exCallback.throwAnyExceptions();

		assertEquals(0, discoveredServices.length);
	}
	
	@Test
	public void requestAssignment() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		byte[] requestBytes = new byte[4];
		MessageBytesUtil.writeInt(requestBytes, 0, 23);
		
		_clusterWorker.requestAssignment(serviceType, requestBytes);
		_exCallback.throwAnyExceptions();
		
		String reqPathRoot = ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), serviceType);
		String reqPath = ZKPaths.makePath(reqPathRoot, _clusterWorker.getMyIdString());
		assertTrue("The request was not created", getTestClient().checkExists().forPath(reqPath) != null);
		byte[] storedReqData = getTestClient().getData().forPath(reqPath);
		assertEquals(4, storedReqData.length);
		assertEquals(23, MessageBytesUtil.readInt(storedReqData, 0));
	}
	
	@Test
	public void requestAssignmentTwiceOver() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		byte[] requestBytes1 = new byte[4];
		MessageBytesUtil.writeInt(requestBytes1, 0, 23);
		byte[] requestBytes2 = new byte[4];
		MessageBytesUtil.writeInt(requestBytes2, 0, 45);
		
		_clusterWorker.requestAssignment(serviceType, requestBytes1);
		_clusterWorker.requestAssignment(serviceType, requestBytes2);
		_exCallback.throwAnyExceptions();
		
		String reqPathRoot = ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), serviceType);
		String reqPath = ZKPaths.makePath(reqPathRoot, _clusterWorker.getMyIdString());
		assertTrue("The request was not created", getTestClient().checkExists().forPath(reqPath) != null);
		byte[] storedReqData = getTestClient().getData().forPath(reqPath);
		assertEquals(4, storedReqData.length);
		assertEquals(45, MessageBytesUtil.readInt(storedReqData, 0));
	}
	
	@Test
	public void getAssignment() throws Throwable {
		_clusterWorker.start();
		
		byte[] resData = new byte[4];
		MessageBytesUtil.writeInt(resData, 0, 42356);
		
		String serviceType = "TEST";
		String resPathRoot = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), serviceType);
		String serviceResPath = ZKPaths.makePath(resPathRoot, _clusterWorker.getMyIdString());
		getTestClient().create().creatingParentsIfNeeded().forPath(resPathRoot);
		getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(serviceResPath, resData);
		
		assertArrayEquals(resData, _clusterWorker.getAssignment(serviceType));
		_exCallback.throwAnyExceptions();
	}
	
	@Test
	public void getAssignmentNoAssignment() throws Throwable {
		_clusterWorker.start();
		
		String serviceType = "TEST";
		
		assertArrayEquals(null, _clusterWorker.getAssignment(serviceType));
		_exCallback.throwAnyExceptions();
	}
	
	@Test
	public void getAssignmentNoServiceType() throws Throwable {
		_clusterWorker.start();
		
		assertArrayEquals(null, _clusterWorker.getAssignment("NotAService"));
		_exCallback.throwAnyExceptions();
	}
	
	@Test
	public void getAssignmentWithDifferentReqServiceTypes() throws Throwable {
		_clusterWorker.start();
		
		byte[] resData1 = new byte[4];
		MessageBytesUtil.writeInt(resData1, 0, 42356);
		
		String serviceType1 = "TEST1";
		String resPathRoot1 = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), serviceType1);
		String serviceResPath1 = ZKPaths.makePath(resPathRoot1, _clusterWorker.getMyIdString());
		getTestClient().create().creatingParentsIfNeeded().forPath(resPathRoot1);
		getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(serviceResPath1, resData1);
		
		byte[] resData2 = new byte[4];
		MessageBytesUtil.writeInt(resData2, 0, 76345);
		
		String serviceType2 = "TEST2";
		String resPathRoot2 = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), serviceType2);
		String serviceResPath2 = ZKPaths.makePath(resPathRoot2, _clusterWorker.getMyIdString());
		getTestClient().create().creatingParentsIfNeeded().forPath(resPathRoot2);
		getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(serviceResPath2, resData2);
		
		assertArrayEquals(resData1, _clusterWorker.getAssignment(serviceType1));
		_exCallback.throwAnyExceptions();
	}
	
	@Test
	public void deleteAssignmentRequest() throws Throwable {
		_clusterWorker.start();

		String serviceType = "TEST";
		byte[] requestBytes = new byte[4];
		MessageBytesUtil.writeInt(requestBytes, 0, 23);
		
		String reqPathRoot = ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), serviceType);
		String reqPath = ZKPaths.makePath(reqPathRoot, _clusterWorker.getMyIdString());
		getTestClient().create().creatingParentsIfNeeded().forPath(reqPathRoot);
		getTestClient().create().withMode(CreateMode.EPHEMERAL).forPath(reqPath, requestBytes);
		
		_clusterWorker.deleteAssignmentRequest(serviceType);
		_exCallback.throwAnyExceptions();
		
		assertTrue("Request path still exists", getTestClient().checkExists().forPath(reqPath) == null);
	}
	
	@Test
	public void deleteAssignmentRequestNoRequest() throws Throwable {
		_clusterWorker.start();

		String serviceType = "TEST";
		
		_clusterWorker.deleteAssignmentRequest(serviceType);
		_exCallback.throwAnyExceptions();
	}
}
