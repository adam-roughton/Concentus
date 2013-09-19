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

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.javatuples.Pair;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.esotericsoftware.minlog.Log;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.utils.ZKPaths;

import static java.lang.Math.*;

public class ClusterUtil {

	public static void ensurePathCreated(
			CuratorFramework client, 
			String path, 
			FatalExceptionCallback exHandler) {
		try {
			ensurePathCreated(client, path);
		} catch (Exception e) {
			exHandler.signalFatalException(e);
		}
	}
	
	public static void ensurePathCreated(
			CuratorFramework client, 
			String path) throws Exception {
		ensurePathCreated(client, path, CreateMode.PERSISTENT);
	}
	
	public static void ensureEphemeralPathCreated(
			CuratorFramework client, 
			String path, 
			FatalExceptionCallback exHandler) {
		try {
			ensurePathCreated(client, path, CreateMode.EPHEMERAL);
		} catch (Exception e) {
			exHandler.signalFatalException(e);
		}
	}
	
	public static void ensureEphemeralPathCreated(
			CuratorFramework client, 
			String path) throws Exception {
		ensurePathCreated(client, path, CreateMode.EPHEMERAL);
	}
	
	private static void ensurePathCreated(
			CuratorFramework client, 
			String path,
			CreateMode mode) throws Exception {
		if (client.checkExists().forPath(path) == null) {
			try {
				client.create().creatingParentsIfNeeded().withMode(mode).forPath(path, null);
			} catch (KeeperException eKeeper) {
				if (eKeeper.code() != KeeperException.Code.NODEEXISTS) {
					throw eKeeper;
				}
			}
		}
	}
	
	public static boolean waitForExistence(final CuratorFramework client, 
			final String path, long timeout, TimeUnit unit) throws Exception {
		final AtomicBoolean exists = new AtomicBoolean(false);
		
		final CountDownLatch latch = new CountDownLatch(1);
		final BackgroundCallback callback = new BackgroundCallback() {
			
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event)
					throws Exception {
				if (event.getType() == CuratorEventType.EXISTS && event.getStat() != null) {
					exists.set(true);
					latch.countDown();
				}
			}
		};
		final CuratorWatcher watcher = new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				if (!exists.get() && event.getType() == EventType.NodeCreated) {
					client.checkExists().usingWatcher(this).inBackground(callback).forPath(path);					
				}
			}
		};
		client.checkExists().usingWatcher(watcher).inBackground(callback).forPath(path);
		
		latch.await(timeout, unit);
		return exists.get();
	}
	
	public static interface DataComparisonDelegate<TData> {
		boolean matches(TData expected, byte[] actual);
	}
	
	public static boolean waitForData(final CuratorFramework client, 
			final String path, final byte[] expectedData, long timeout, TimeUnit unit) throws Exception {
		DataComparisonDelegate<byte[]> compDelegate = new DataComparisonDelegate<byte[]>() {

			@Override
			public boolean matches(byte[] expected, byte[] actual) {
				return Arrays.equals(expected, actual);
			}
		};
		return waitForData(client, path, expectedData, compDelegate, timeout, unit);
	}
	
	public static <TData> boolean waitForData(final CuratorFramework client, 
			final String path, final TData expectedData, final DataComparisonDelegate<TData> compDelegate, 
			long timeout, TimeUnit unit) throws Exception {
		long startTime = System.currentTimeMillis();
		long deadline = startTime + unit.toMillis(timeout);
		if (!waitForExistence(client, path, timeout, unit)) {
			return false;
		}

		final AtomicBoolean matched = new AtomicBoolean(false);
		
		final CountDownLatch latch = new CountDownLatch(1);
		final BackgroundCallback callback = new BackgroundCallback() {
			
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event)
					throws Exception {
				if (event.getType() == CuratorEventType.GET_DATA && 
						compDelegate.matches(expectedData, event.getData())) {
					matched.set(true);
					latch.countDown();
				}
			}
		};
		final CuratorWatcher watcher = new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				if (!matched.get() && event.getType() == EventType.NodeDataChanged) {
					client.getData().usingWatcher(this).inBackground(callback).forPath(path);					
				}
			}
		};
		client.getData().usingWatcher(watcher).inBackground(callback).forPath(path);
		
		latch.await(max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
		return matched.get();
	}
	
	public static List<String> waitForChildren(final CuratorFramework client, 
			final String path, final int count, long timeout, TimeUnit unit) throws Exception {
		ensurePathCreated(client, path);
		final AtomicReference<List<String>> childrenRef = new AtomicReference<>(null);		
		
		final CountDownLatch latch = new CountDownLatch(1);
		final BackgroundCallback callback = new BackgroundCallback() {
			
			@Override
			public void processResult(CuratorFramework client, CuratorEvent event)
					throws Exception {
				if (event.getType() == CuratorEventType.CHILDREN) {
					List<String> children = event.getChildren();
					Log.info("waitForChildren: Got child count " + children.size());
					if (children.size() >= count) {
						if (childrenRef.compareAndSet(null, new ArrayList<>(children))) {
							Log.info("waitForChildren: got req. count");
							latch.countDown();
						}
					}
				}
			}
		};
		final CuratorWatcher watcher = new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				Log.info("waitForChildren: watch event " + event.toString());
				if (event.getType() == EventType.NodeChildrenChanged) {
					client.getChildren().usingWatcher(this).inBackground(callback).forPath(path);					
				}
				
			}
		};
		client.getChildren().usingWatcher(watcher).inBackground(callback).forPath(path);
		
		latch.await(timeout, unit);
		List<String> children = childrenRef.get();
		if (children == null) {
			throw new TimeoutException();
		} else {
			return children;
		}
	}
	
	public static List<String> toPaths(String basePath, List<String> nodeNames) { 
		List<String> paths = new ArrayList<>();
		for (String nodeName : nodeNames) {
			paths.add(ZKPaths.makePath(basePath, nodeName));
		}
		return paths;
	}
	
	/**
	 * Validates the cluster state and returns a {@link Pair} containing the state signalling that the service
	 * has been created ({@link ClusterState#CREATED_STATE_CODE}), and a map that links code values to states
	 * for reverse lookup.
	 * @param stateType the type of the state
	 * @return {@link Pair} containing the state signalling that the service
	 * has been created ({@link ClusterState#CREATED_STATE_CODE}), and a map that links code values to states
	 * for reverse lookup
	 */
	public static <TState extends Enum<TState> & ClusterState> Pair<TState, Int2ObjectMap<TState>> validateStateType(Class<TState> stateType) {
		TState[] values = stateType.getEnumConstants();
		Int2ObjectMap<TState> codeMap = new Int2ObjectArrayMap<>(values.length);
		for (TState state : values) {
			int stateCode = state.code();
			if (codeMap.containsKey(stateCode)) {
				throw new IllegalArgumentException("The state type " + stateType.getName().toString() + " " +
						"defines two or more states using the same code (" + stateCode + ").");
			} else {
				codeMap.put(stateCode, state);
			}
		}
		if (!codeMap.containsKey(ClusterState.CREATED_STATE_CODE)) {
			throw new IllegalArgumentException("The state type " + stateType.getName().toString() + " " +
					"did not define a CREATED_STATE state (with code " + ClusterState.CREATED_STATE_CODE + ").");
		}
		return new Pair<>(codeMap.get(ClusterState.CREATED_STATE_CODE), codeMap);
	}
	
}
