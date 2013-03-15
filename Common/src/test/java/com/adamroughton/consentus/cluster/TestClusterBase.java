package com.adamroughton.consentus.cluster;

import org.junit.After;
import org.junit.Before;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;

public abstract class TestClusterBase {

	public static final String ROOT = "/Consentus";
	private static final int DEFAULT_PORT = 5000;
	
	private final int _port;
	private CuratorFramework _client;
	private TestingServer _zooKeeperServer;
	
	public TestClusterBase(int port) {
		_port = port;
	}
	
	public TestClusterBase() {
		this(DEFAULT_PORT);
	}
	
	@Before
	public final void setUpZooKeeper() throws Exception {
		_zooKeeperServer = new TestingServer(_port);
		_client = CuratorFrameworkFactory.newClient(getZooKeeperAddress(), 
				new ExponentialBackoffRetry(10, 3));
		_client.start();
	}
	
	@After
	public final void tearDownZooKeeper() throws Exception {
		_client.close();
		_zooKeeperServer.close();			
	}
	
	protected String getZooKeeperAddress() {
		return "127.0.0.1:" + _port;
	}
	
	/**
	 * Gets the client allocated for reading ZooKeeper
	 * data during the test.
	 * @return the test client for reading ZooKeeper data
	 */
	protected CuratorFramework getTestClient() {
		return _client;
	}
	
	
	
}
