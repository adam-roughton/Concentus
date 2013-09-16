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
import com.netflix.curator.utils.ZKPaths;

public abstract class TestClusterBase {

	public static final String ROOT = "/Concentus";
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
	
	protected String makeTestPath(String name) {
		return ZKPaths.makePath(ZKPaths.makePath(ROOT, "testPaths"), name);
	}
	
}
