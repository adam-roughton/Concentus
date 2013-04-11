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
package com.adamroughton.concentus.cluster.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.Util;
import com.adamroughton.concentus.cluster.ClusterPath;
import com.adamroughton.concentus.cluster.ClusterState;
import com.adamroughton.concentus.cluster.ExceptionCallback;
import com.adamroughton.concentus.cluster.TestClusterBase;
import com.adamroughton.concentus.cluster.worker.ClusterStateNodeListener;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

import static org.junit.Assert.*;

public class TestClusterStateNodeListener extends TestClusterBase {

	private ExecutorService _executor;
	private StateChangeHandler _stateChangeHandler;
	private Disruptor<byte[]> _disruptor;
	private ClusterStateNodeListener _nodeListener;
	private ExceptionCallback _exCallback;
	private CuratorFramework _client;
	
	private final static String STATE_PATH = ClusterPath.STATE.getPath(ROOT);
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
		_executor = Executors.newCachedThreadPool();
		_disruptor = new Disruptor<>(Util.msgBufferFactory(8), 32, _executor);
		_stateChangeHandler = new StateChangeHandler();
		_disruptor.handleEventsWith(_stateChangeHandler);
		_disruptor.start();
		
		_client = CuratorFrameworkFactory.newClient(getZooKeeperAddress(), new ExponentialBackoffRetry(10, 3));
		_client.start();
		_exCallback = new ExceptionCallback();
		_nodeListener = new ClusterStateNodeListener(_client, STATE_PATH, _disruptor, _exCallback);
	}
	
	@After
	public void tearDown() {
		_disruptor.shutdown();
		_client.close();
	}
	
	@Test
	public void initialStateIsActedOn() throws Exception {
		ClusterState expected = new ClusterState(3, 2);
		
		getTestClient().create().creatingParentsIfNeeded().forPath(STATE_PATH, ClusterState.toBytes(expected));
		_nodeListener.start();
		
		_stateChangeHandler.waitForCount(1, 1000, TimeUnit.MILLISECONDS);
		
		List<ClusterState> clusterStates = _stateChangeHandler.getChanges();
		assertEquals(1, clusterStates.size());
		assertEquals(expected, clusterStates.get(0));
	}
	
	@Test
	public void nodeChangedOnce() throws Exception {
		ClusterState expected = new ClusterState(3, 2);
		
		getTestClient().create().creatingParentsIfNeeded().forPath(STATE_PATH, null);
		_nodeListener.start();
		
		getTestClient().setData().forPath(STATE_PATH, ClusterState.toBytes(expected));
		
		_stateChangeHandler.waitForCount(1, 1000, TimeUnit.SECONDS);
		
		List<ClusterState> clusterStates = _stateChangeHandler.getChanges();
		assertEquals(1, clusterStates.size());
		assertEquals(expected, clusterStates.get(0));
	}
	
	@Test
	public void nodeChangedManyItems() throws Exception {
		getTestClient().create().creatingParentsIfNeeded().forPath(STATE_PATH);
		_nodeListener.start();
		
		List<ClusterState> expected = Arrays.asList(
				new ClusterState(3, 2), 
				new ClusterState(3, 5), 
				new ClusterState(3, 10));
		
		int lastCount = 0;
		for (ClusterState state : expected) {
			getTestClient().setData().forPath(STATE_PATH, ClusterState.toBytes(state));
			_stateChangeHandler.waitForCount(++lastCount, 1000, TimeUnit.MILLISECONDS);
		}
		
		_stateChangeHandler.waitForCount(expected.size(), 1000, TimeUnit.MILLISECONDS);
		
		List<ClusterState> clusterStates = _stateChangeHandler.getChanges();
		assertEquals(expected, clusterStates);
	}
	
	@Test
	public void nodeChangedInitialNullData() throws Exception {
		getTestClient().create().creatingParentsIfNeeded().forPath(STATE_PATH, null);
		_nodeListener.start();
		
		// wait to give time for processing, continuing immediately if we get a false hit
		_stateChangeHandler.waitForCount(1, 1000, TimeUnit.MILLISECONDS);
		
		List<ClusterState> clusterStates = _stateChangeHandler.getChanges();
		assertEquals(0, clusterStates.size());
	}
	
	@Test
	public void nodeChangedNullData() throws Exception {
		getTestClient().create().creatingParentsIfNeeded().forPath(STATE_PATH);
		_nodeListener.start();
		
		List<ClusterState> expected = Arrays.asList(
				new ClusterState(3, 2), 
				new ClusterState(3, 5), 
				new ClusterState(3, 10));
		
		// alternate real state and nulls
		int changesCount = expected.size() * 2;
		List<ClusterState> changes = new ArrayList<>(changesCount);
		for (int i = 0; i < changesCount; i++) {
			if (i % 2 == 0) {
				changes.add(expected.get(i / 2));
			} else {
				changes.add(null);
			}
		}
		
		int lastCount = 0;
		for (ClusterState state : changes) {
			getTestClient().setData().forPath(STATE_PATH, state != null? ClusterState.toBytes(state) : null);
			if (state != null) {
				_stateChangeHandler.waitForCount(++lastCount, 1000, TimeUnit.MILLISECONDS);
			} else {
				// wait to give time for processing, continuing immediately if we get a false hit
				_stateChangeHandler.waitForCount(lastCount + 1, 1000, TimeUnit.MILLISECONDS);
			}
		}
		
		_stateChangeHandler.waitForCount(expected.size(), 1000, TimeUnit.MILLISECONDS);
		
		List<ClusterState> clusterStates = _stateChangeHandler.getChanges();
		assertEquals(expected, clusterStates);
	}
	
	private static class StateChangeHandler implements EventHandler<byte[]> {
		
		private final ValueCollector<ClusterState> _valueCollector = new ValueCollector<>();
		private Exception _thrownException = null;
		
		@Override
		public synchronized void onEvent(byte[] event, long sequence, boolean endOfBatch)
				throws Exception {
			try {
				_valueCollector.addValue(ClusterState.fromBytes(event));
			} catch (Exception e) {
				if (_thrownException == null) {
					_thrownException = e;
				}
			}			
		}
		
		public synchronized List<ClusterState> getChanges() throws Exception {
			if (_thrownException != null) throw _thrownException;
			return _valueCollector.getValues();
		}
		
		public void waitForCount(int count, long timeout, TimeUnit unit) throws InterruptedException {
			_valueCollector.waitForCount(count, timeout, unit);
		}
	}
	
}
