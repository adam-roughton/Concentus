package com.adamroughton.consentus.cluster;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingServer;

public abstract class TestClusterBase {

	public static final String ROOT = "/Consentus";
	public static final int DEFAULT_PORT = 5000;
	
	private CuratorFramework _client;
	private TestingServer _zooKeeperServer;
	
	// manage the tempDir directly so that we ensure it is cleared
	// before each test run
	private static Path _tmpPath;
	
	@BeforeClass
	public static void setUpTestEnv() throws Exception {
		_tmpPath = Files.createTempDirectory("Consentus");
	}
	
	@AfterClass
	public static void tearDownTestEnv() {
		FileUtils.deleteQuietly(_tmpPath.toFile());
	}
	
	@Before
	public final void setUpZooKeeper() throws Exception {
		FileUtils.cleanDirectory(_tmpPath.toFile());
		InstanceSpec instanceSpec = new InstanceSpec(_tmpPath.toFile(), -1, -1, -1, false, -1);
		_zooKeeperServer = new TestingServer(instanceSpec);
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
		return _zooKeeperServer.getConnectString();
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
