package com.adamroughton.consentus.cluster;


import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;

import static org.junit.Assert.*;

public class TestClusterParticipant extends TestClusterBase {
	
	private ExceptionCallback _exCallback;
	private ClusterParticipantAccessWrapper _clusterParticipant;
	
	@Before
	public void setUp() throws Exception {
		_exCallback = new ExceptionCallback();
		_clusterParticipant = new ClusterParticipantAccessWrapper(getZooKeeperAddress(), ROOT, _exCallback);
	}
	
	@After
	public void tearDown() throws Exception {
		_clusterParticipant.close();
	}
	
	@Test
	public void createPaths() throws Throwable {
		_clusterParticipant.start();
		
		CuratorFramework testClient = getTestClient();
		for (ClusterPath path : ClusterPath.values()) {
			assertTrue(testClient.checkExists().forPath(path.getPath(ROOT)) != null);
		}
		_exCallback.throwAnyExceptions();
	}
	
	@Test
	public void createPathsAlreadyExist() throws Throwable {
		CuratorFramework testClient = getTestClient();
		for (ClusterPath path : ClusterPath.values()) {
			testClient.create().creatingParentsIfNeeded().forPath(path.getPath(ROOT));
		}
		
		_clusterParticipant.start();
		for (ClusterPath path : ClusterPath.values()) {
			assertTrue(testClient.checkExists().forPath(path.getPath(ROOT)) != null);
		}
		_exCallback.throwAnyExceptions();
	}
	
	@Test
	public void createEphemeralNode() throws Exception {
		_clusterParticipant.start();
		
		String testPath = ZKPaths.makePath(ClusterPath.ASSIGN_RES.getPath(ROOT), "abcdefg");
		byte[] testData = new byte[4];
		MessageBytesUtil.writeInt(testData, 0, 100);
		
		_clusterParticipant.createOrSetEphemeral(testPath, testData);
		
		CuratorFramework testClient = getTestClient();
		assertTrue("Ephemeral node was not created", testClient.checkExists().forPath(testPath) != null);
		
		byte[] storedTestData = testClient.getData().forPath(testPath);
		assertEquals(100, MessageBytesUtil.readInt(storedTestData, 0));
		
		_clusterParticipant.close();
		
		assertTrue("Ephemeral node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	@Test
	public void deleteStandardNode() throws Exception {
		_clusterParticipant.start();
		CuratorFramework testClient = getTestClient();
		
		String testPath = ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), "abcdefg");
		byte[] testData = new byte[4];
		MessageBytesUtil.writeInt(testData, 0, 100);
		testClient.create().creatingParentsIfNeeded().forPath(testPath);
		
		assertTrue("Failed setting up test conditions (failed to make standard node)", testClient.checkExists().forPath(testPath) != null);
		
		_clusterParticipant.delete(testPath);
		
		assertTrue("Node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	@Test
	public void deleteEphemeralNode() throws Exception {
		_clusterParticipant.start();
		CuratorFramework testClient = getTestClient();
		
		String testPath = ZKPaths.makePath(ClusterPath.ASSIGN_REQ.getPath(ROOT), "abcdefg");
		byte[] testData = new byte[4];
		MessageBytesUtil.writeInt(testData, 0, 100);
		testClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(testPath);
		
		assertTrue("Failed setting up test conditions (failed to make ephemeral node)", testClient.checkExists().forPath(testPath) != null);
		
		_clusterParticipant.delete(testPath);
		
		assertTrue("Node has not been deleted", testClient.checkExists().forPath(testPath) == null);
	}
	
	private static class ClusterParticipantAccessWrapper extends ClusterParticipant {
		
		private final static UUID PARTICIPANT_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
		
		public ClusterParticipantAccessWrapper(String zooKeeperAddress, String root,
				FatalExceptionCallback exHandler) {
			super(zooKeeperAddress, root, PARTICIPANT_ID, exHandler);
		}

		@Override
		public String getPath(ClusterPath pathType) {
			return super.getPath(pathType);
		}

		@Override
		public CuratorFramework getClient() {
			return super.getClient();
		}

		@Override
		public FatalExceptionCallback getExHandler() {
			return super.getExHandler();
		}
	}
	
}
