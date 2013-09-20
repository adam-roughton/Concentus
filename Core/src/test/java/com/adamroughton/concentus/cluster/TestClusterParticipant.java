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
package com.adamroughton.concentus.cluster;

import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.netflix.curator.framework.CuratorFramework;

import static org.junit.Assert.*;

public class TestClusterParticipant extends TestClusterBase {
	
	private final static UUID PARTICIPANT_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
	
	private ExceptionCallback _exCallback;
	private ClusterParticipant _clusterParticipant;
	private Kryo _kryo;
	
	private static class TestType implements TestTypeInterface {
		long value;

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof TestType)) {
				return false;
			}
			TestType other = (TestType) obj;
			if (value != other.value) {
				return false;
			}
			return true;
		}
		
	}
	
	public static interface TestTypeInterface {
		boolean equals(Object obj);
	}
	
	public static class TestTypeSubClass extends TestType {
	}
	
	@Before
	public void setUp() throws Exception {
		_exCallback = new ExceptionCallback();
		KryoRegistratorDelegate registrator = new KryoRegistratorDelegate() {
			
			@Override
			public void register(Kryo kryo) {
				int nextId = DataType.nextFreeId();
				kryo.register(TestType.class, nextId++);
				kryo.register(TestTypeSubClass.class, nextId++);
				
			}
		};
		_kryo = Util.newKryoInstance();
		registrator.register(_kryo);
		_clusterParticipant = new ClusterParticipant(new ClusterHandleSettings(getZooKeeperAddress(), ROOT, PARTICIPANT_ID, registrator, _exCallback));
	}
	
	@After
	public void tearDown() throws Exception {
		_clusterParticipant.close();
	}
	
	@Test
	public void createEphemeralNode() throws Throwable {
		_clusterParticipant.start();
		
		
		String testPath = makeTestPath("test1");
		int testData = 100;
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		_exCallback.throwAnyExceptions();
		
		CuratorFramework testClient = getTestClient();
		assertTrue("Ephemeral node was not created", testClient.checkExists().forPath(testPath) != null);
		
		byte[] storedTestData = testClient.getData().forPath(testPath);
		Input input = new Input(storedTestData);
		Object storedTestObj = _kryo.readClassAndObject(input);
		assertTrue(storedTestObj instanceof Integer);
		assertEquals(100, ((Integer) storedTestObj).intValue());
		
		_clusterParticipant.close();
		
		assertTrue("Ephemeral node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	@Test
	public void createEphemeralNodeNullData() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");	
		_clusterParticipant.createOrSetEphemeral(testPath, null);
		_exCallback.throwAnyExceptions();
		
		CuratorFramework testClient = getTestClient();
		assertTrue("Ephemeral node was not created", testClient.checkExists().forPath(testPath) != null);
		
		byte[] storedTestData = testClient.getData().forPath(testPath);
		Input input = new Input(storedTestData);
		assertNull(_kryo.readClassAndObject(input));
		
		_clusterParticipant.close();
		
		assertTrue("Ephemeral node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	@Test
	public void setEphemeralNode() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		int testData1 = 100;
		int testData2 = 200;
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData1);
		_clusterParticipant.createOrSetEphemeral(testPath, testData2);
		_exCallback.throwAnyExceptions();
		
		CuratorFramework testClient = getTestClient();
		assertTrue("Ephemeral node was not created", testClient.checkExists().forPath(testPath) != null);
		
		byte[] storedTestData = testClient.getData().forPath(testPath);
		Input input = new Input(storedTestData);
		Object storedTestObj = _kryo.readClassAndObject(input);
		assertTrue(storedTestObj instanceof Integer);
		assertEquals(200, ((Integer) storedTestObj).intValue());
		
		_clusterParticipant.close();
		
		assertTrue("Ephemeral node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	@Test
	public void createAndReadKryo_KryoEncodedNull() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
	   
		_clusterParticipant.createOrSetEphemeral(testPath, (Object) null);
		Object res = _clusterParticipant.read(testPath, Object.class);
		_exCallback.throwAnyExceptions();
		
		assertNull(res);
	}
	
	@Test
	public void createAndReadKryo_Null() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		ClusterUtil.ensurePathCreated(getTestClient(), testPath);
		getTestClient().setData().forPath(testPath, null);
		
		Object res = _clusterParticipant.read(testPath, Object.class);
		_exCallback.throwAnyExceptions();
		
		assertNull(res);
	}
	
	@Test
	public void createAndReadKryo_Primitive() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		int testData = 400;
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		Integer res = _clusterParticipant.read(testPath, Integer.class);
		_exCallback.throwAnyExceptions();
		
		assertEquals(testData, res.intValue());
	}
	
	@Test
	public void createAndReadKryo_RegisteredObjectType() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		TestType testData = new TestType();
		testData.value = 9;
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		TestType res = _clusterParticipant.read(testPath, TestType.class);
		_exCallback.throwAnyExceptions();
		
		assertEquals(testData, res);
	}
	
	@Test
	public void createAndReadKryo_RegisteredObjectSuperClassType() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		TestType testData = new TestTypeSubClass();
		testData.value = 9;
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		TestType res = _clusterParticipant.read(testPath, TestType.class);
		_exCallback.throwAnyExceptions();
		
		assertEquals(testData, res);
	}
	
	@Test
	public void createAndReadKryo_RegisteredObjectInterfaceType() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		TestType testData = new TestType();
		testData.value = 9;
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		TestTypeInterface res = _clusterParticipant.read(testPath, TestTypeInterface.class);
		_exCallback.throwAnyExceptions();
		
		assertEquals(testData, res);
	}
	
	@Test
	public void createAndReadKryo_UnregisteredObjectType() throws Throwable {
		_clusterParticipant.start();
		
		String testPath = makeTestPath("test1");
		String testData = "hello world!";
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		String res = _clusterParticipant.read(testPath, String.class);
		_exCallback.throwAnyExceptions();
		
		assertEquals(testData, res);
	}
	
	@Test
	public void deleteStandardNode() throws Throwable {
		_clusterParticipant.start();
		CuratorFramework testClient = getTestClient();
		
		String testPath = makeTestPath("test1");
		byte[] testData = new byte[4];
		BytesUtil.writeInt(testData, 0, 100);
		testClient.create().creatingParentsIfNeeded().forPath(testPath);
		
		assertTrue("Failed setting up test conditions (failed to make standard node)", testClient.checkExists().forPath(testPath) != null);
		
		_clusterParticipant.delete(testPath);
		_exCallback.throwAnyExceptions();
		
		assertTrue("Node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	@Test
	public void deleteEphemeralNode() throws Throwable {
		_clusterParticipant.start();
		CuratorFramework testClient = getTestClient();
		
		String testPath = makeTestPath("test1");
		byte[] testData = new byte[4];
		BytesUtil.writeInt(testData, 0, 100);
		testClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(testPath);
		
		assertTrue("Failed setting up test conditions (failed to make ephemeral node)", testClient.checkExists().forPath(testPath) != null);
		
		_clusterParticipant.delete(testPath);
		_exCallback.throwAnyExceptions();
		
		assertTrue("Node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
}
