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
	
	@Test
	public void ensurePathCreatedAlreadyExists() throws Exception {
		String path = ClusterPath.READY.getPath("/Consentus");
		ClusterUtil.ensurePathCreated(_client, path);
		ClusterUtil.ensurePathCreated(_client, path);
		assertTrue(getTestClient().checkExists().forPath(path) != null);
	}
}
