package com.adamroughton.consentus.cluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import static org.junit.Assert.*;

public class TestClusterUtil extends TestClusterBase {

	private CuratorFramework _client;
	
	@Before
	public void setUp() {
		_client = CuratorFrameworkFactory.newClient(getZooKeeperAddress(), new ExponentialBackoffRetry(10, 3));
		_client.start();
	}
	
	@After
	public void tearDown() {
		_client.close();
	}
	
	@Test
	public void ensurePathCreated() throws Exception {
		String path = ClusterPath.READY.getPath("/Consentus");
		ClusterUtil.ensurePathCreated(_client, path);
		assertTrue(getTestClient().checkExists().forPath(path) != null);
	}
	
}
