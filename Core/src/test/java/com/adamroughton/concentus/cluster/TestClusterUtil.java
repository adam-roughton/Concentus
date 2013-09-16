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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.cluster.ClusterUtil;
import com.adamroughton.concentus.util.Util;
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
		String path = makeTestPath("Test1");
		ClusterUtil.ensurePathCreated(_client, path);
		assertTrue(getTestClient().checkExists().forPath(path) != null);
	}
	
	@Test
	public void ensurePathCreatedAlreadyExists() throws Exception {
		String path = makeTestPath("Test1");
		ClusterUtil.ensurePathCreated(_client, path);
		ClusterUtil.ensurePathCreated(_client, path);
		assertTrue(getTestClient().checkExists().forPath(path) != null);
	}
	
	@Test
	public void waitForExistence() throws Throwable {
		final String path = makeTestPath("Test1");
		final ExceptionCallback exCallback = new ExceptionCallback();
		final CountDownLatch latch = new CountDownLatch(1);
		Thread setThread = new Thread() {

			@Override
			public void run() {
				try {
					latch.await();
					Thread.sleep(1000);
					getTestClient().create().creatingParentsIfNeeded().forPath(path);
				} catch (Exception e) {
					exCallback.signalFatalException(e);
				}
			}
			
		};
		setThread.start();
		latch.countDown();
		assertTrue(ClusterUtil.waitForExistence(_client, path, 2000, TimeUnit.MILLISECONDS));
		exCallback.throwAnyExceptions();
	}
	
	@Test
	public void waitForExistenceRepeatSet() throws Throwable {
		final String path = makeTestPath("Test1");
		final ExceptionCallback exCallback = new ExceptionCallback();
		final CountDownLatch latch = new CountDownLatch(1);
		Thread setThread = new Thread() {

			@Override
			public void run() {
				try {
					latch.await();
					Thread.sleep(1000);
					getTestClient().create().creatingParentsIfNeeded().forPath(path);
					for (int i = 0; i < 5; i++) {
						getTestClient().setData().forPath(path, Util.intToBytes(i));
					}
				} catch (Exception e) {
					exCallback.signalFatalException(e);
				}
			}
			
		};
		setThread.start();
		latch.countDown();
		assertTrue(ClusterUtil.waitForExistence(_client, path, 2000, TimeUnit.MILLISECONDS));
		exCallback.throwAnyExceptions();
	}
	
	@Test
	public void waitForExistenceTimeout() throws Throwable {
		final String path = makeTestPath("Test1");
		assertFalse(ClusterUtil.waitForExistence(_client, path, 1000, TimeUnit.MILLISECONDS));
	}
	
	@Test
	public void waitForData() throws Throwable {
		final String path = makeTestPath("Test1");
		final ExceptionCallback exCallback = new ExceptionCallback();
		final CountDownLatch latch = new CountDownLatch(1);
		Thread setThread = new Thread() {

			@Override
			public void run() {
				try {
					latch.await();
					Thread.sleep(1000);
					getTestClient().create().creatingParentsIfNeeded().forPath(path);
					getTestClient().setData().forPath(path, Util.intToBytes(15));
				} catch (Exception e) {
					exCallback.signalFatalException(e);
				}
			}
			
		};
		setThread.start();
		latch.countDown();
		assertTrue(ClusterUtil.waitForData(_client, path, Util.intToBytes(15), 2000, TimeUnit.MILLISECONDS));
		exCallback.throwAnyExceptions();
	}
	
	@Test
	public void waitForDataSetMultiple() throws Throwable {
		final String path = makeTestPath("Test1");
		final ExceptionCallback exCallback = new ExceptionCallback();
		final CountDownLatch latch = new CountDownLatch(1);
		Thread setThread = new Thread() {

			@Override
			public void run() {
				try {
					latch.await();
					Thread.sleep(1000);
					getTestClient().create().creatingParentsIfNeeded().forPath(path);
					for (int i = 0; i < 10; i++) {
						getTestClient().setData().forPath(path, Util.intToBytes(10 + i));
					}
				} catch (Exception e) {
					exCallback.signalFatalException(e);
				}
			}
			
		};
		setThread.start();
		latch.countDown();
		assertTrue(ClusterUtil.waitForData(_client, path, Util.intToBytes(15), 2000, TimeUnit.MILLISECONDS));
		exCallback.throwAnyExceptions();
	}
	
	@Test
	public void waitForDataSetMultipleNoMatch() throws Throwable {
		final String path = makeTestPath("Test1");
		final ExceptionCallback exCallback = new ExceptionCallback();
		final CountDownLatch latch = new CountDownLatch(1);
		Thread setThread = new Thread() {

			@Override
			public void run() {
				try {
					latch.await();
					Thread.sleep(1000);
					getTestClient().create().creatingParentsIfNeeded().forPath(path);
					for (int i = 0; i < 10; i++) {
						getTestClient().setData().forPath(path, Util.intToBytes(20 + i));
					}
				} catch (Exception e) {
					exCallback.signalFatalException(e);
				}
			}
			
		};
		setThread.start();
		latch.countDown();
		assertFalse(ClusterUtil.waitForData(_client, path, Util.intToBytes(15), 2000, TimeUnit.MILLISECONDS));
		exCallback.throwAnyExceptions();
	}
	
	@Test
	public void waitForDataNotSetWithNullExpected() throws Throwable {
		final String path = makeTestPath("Test1");
		assertFalse(ClusterUtil.waitForData(_client, path, null, 2000, TimeUnit.MILLISECONDS));
	}
	
	@Test
	public void waitForDataTimeout() throws Throwable {
		final String path = makeTestPath("Test1");
		assertFalse(ClusterUtil.waitForData(_client, path, Util.intToBytes(15), 1000, TimeUnit.MILLISECONDS));
	}
	
}
