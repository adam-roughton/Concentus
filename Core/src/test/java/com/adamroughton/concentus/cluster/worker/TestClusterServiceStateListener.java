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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.ExceptionCallback;
import com.adamroughton.concentus.cluster.TestClusterBase;
import com.adamroughton.concentus.cluster.worker.ClusterServiceStateSignalListener;
import com.adamroughton.concentus.cluster.worker.ClusterServiceStateSignalListener.ListenerDelegate;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.data.cluster.kryo.ServiceState.*;
import static org.junit.Assert.*;

public class TestClusterServiceStateListener extends TestClusterBase {

	private ClusterStateListener<ServiceState> _listener;
	private ClusterServiceStateSignalListener<ServiceState> _nodeListener;
	private ExceptionCallback _exCallback;
	private CuratorFramework _client;
	private Kryo _kryo;
	
	private final String _servicePath;
	private final String _signalPath;
	
	public TestClusterServiceStateListener() {
		_servicePath = ZKPaths.makePath(CorePath.SERVICES.getAbsolutePath(ROOT), "myIdString1234");
		_signalPath = CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(_servicePath);
	}
	
	@Before
	public void setUp() {
		_listener = new ClusterStateListener<ServiceState>();
		_kryo = Util.newKryoInstance();
		
		_client = CuratorFrameworkFactory.newClient(getZooKeeperAddress(), new ExponentialBackoffRetry(10, 3));
		_client.start();
		_exCallback = new ExceptionCallback();
		_nodeListener = new ClusterServiceStateSignalListener<>(ServiceState.class, _client, _signalPath, _listener, _exCallback);
	}
	
	@After
	public void tearDown() {
		_client.close();
	}
	
	private void setState(ServiceState state) throws Exception {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		Output output = new Output(bOut);
		_kryo.writeClassAndObject(output, state);
		output.close();
		
		byte[] data = bOut.toByteArray();
		if (getTestClient().checkExists().forPath(_signalPath) != null) {
			getTestClient().setData().forPath(_signalPath, data);
		} else {
			getTestClient().create().creatingParentsIfNeeded().forPath(_signalPath, data);			
		}
		
	}
	
	@Test
	public void initialNullStateIsIgnored() throws Exception {
		setState(null);
		_nodeListener.start();
		
		ValueCollector<ServiceState> collector = _listener.getValueCollector();
		Thread.sleep(1000);
		
		List<ServiceState> clusterStates = collector.getValues();
		assertEquals(0, clusterStates.size());
	}
	
	@Test
	public void initialStateIsActedOn() throws Exception {
		setState(CONNECT);
		_nodeListener.start();
		
		ValueCollector<ServiceState> collector = _listener.getValueCollector();
		collector.waitForCount(1, 1000, TimeUnit.MILLISECONDS);
		
		List<ServiceState> clusterStates = collector.getValues();
		assertEquals(1, clusterStates.size());
		assertEquals(CONNECT, clusterStates.get(0));
	}
	
	@Test
	public void nodeChangedOnce() throws Exception {
		setState(null);
		_nodeListener.start();
		
		setState(START);
		
		ValueCollector<ServiceState> collector = _listener.getValueCollector();
		collector.waitForCount(1, 1000, TimeUnit.MILLISECONDS);
		
		List<ServiceState> clusterStates = collector.getValues();
		assertEquals(1, clusterStates.size());
		assertEquals(START, clusterStates.get(0));
	}
	
	@Test
	public void nodeChangedManyItems() throws Exception {
		setState(null);
		_nodeListener.start();
		ValueCollector<ServiceState> collector = _listener.getValueCollector();
		
		List<ServiceState> expected = Arrays.asList(
				START, 
				BIND, 
				CONNECT);
				
		int lastCount = 0;
		for (ServiceState state : expected) {
			setState(state);
			collector.waitForCount(++lastCount, 1000, TimeUnit.MILLISECONDS);
		}
		
		collector.waitForCount(expected.size(), 1000, TimeUnit.MILLISECONDS);
		
		List<ServiceState> clusterStates = collector.getValues();
		assertEquals(expected, clusterStates);
	}
		
	@Test
	public void nodeChangedNullData() throws Exception {
		setState(null);
		_nodeListener.start();
		ValueCollector<ServiceState> collector = _listener.getValueCollector();
		
		List<ServiceState> expected = Arrays.asList(
				START, 
				BIND, 
				CONNECT);
		
		// alternate real state and nulls
		int changesCount = expected.size() * 2;
		List<ServiceState> changes = new ArrayList<>(changesCount);
		for (int i = 0; i < changesCount; i++) {
			if (i % 2 == 0) {
				changes.add(expected.get(i / 2));
			} else {
				changes.add(null);
			}
		}
		
		int lastCount = 0;
		for (ServiceState state : changes) {
			setState(state);
			if (state != null) {
				collector.waitForCount(++lastCount, 1000, TimeUnit.MILLISECONDS);
			} else {
				// wait to give time for processing, continuing immediately if we get a false hit
				collector.waitForCount(lastCount + 1, 1000, TimeUnit.MILLISECONDS);
			}
		}
		
		collector.waitForCount(expected.size(), 1000, TimeUnit.MILLISECONDS);
		
		List<ServiceState> clusterStates = collector.getValues();
		assertEquals(expected, clusterStates);
	}
	
	private static class ClusterStateListener<TState> implements ListenerDelegate<TState> {
		
		private final ValueCollector<TState> _valueCollector;
		
		public ClusterStateListener() {
			_valueCollector = new ValueCollector<>();
		}
		
		@Override
		public void onStateChanged(TState newState) throws Exception {
			_valueCollector.addValue(newState);
		}
		
		public ValueCollector<TState> getValueCollector() {
			return _valueCollector;
		}
		
	}
	
	
	
}
