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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;

public class ClusterParticipant implements Closeable {

	private final FatalExceptionCallback _exHandler;
	
	private final UUID _clusterParticipantId;
	private final CuratorFramework _client;
	private final String _root;
	private final Kryo _kryo;
	
	public ClusterParticipant(
			String zooKeeperAddress, 
			String root,
			UUID clusterId,
			FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, clusterId, null, exHandler);
	}
	
	public ClusterParticipant(
			String zooKeeperAddress, 
			String root,
			UUID clusterId,
			KryoRegistratorDelegate kryoRegistrator,
			FatalExceptionCallback exHandler) {
		_exHandler = Objects.requireNonNull(exHandler);
		_client = CuratorFrameworkFactory.newClient(zooKeeperAddress, new ExponentialBackoffRetry(1000, 3));
		_clusterParticipantId = Objects.requireNonNull(clusterId);
		_root = Objects.requireNonNull(root);
		
		_kryo = Util.newKryoInstance();
		if (kryoRegistrator != null) {
			kryoRegistrator.register(_kryo);
		}
		
		if (!Util.isValidZKRoot(root)) {
			throw new IllegalArgumentException(String.format("The root %s is not a valid ZooKeeper root.", root));
		}
	}
	
	public UUID getMyId() {
		return _clusterParticipantId;
	}
	
	public String getMyIdString() {
		return _clusterParticipantId.toString();
	}
	
	public String resolvePathFromRoot(ClusterPath path) {
		if (!path.isRelativeToRoot()) {
			throw new IllegalArgumentException("The path cannot be resolved as it " +
					"is not relative to the application root");
		}
		return path.getAbsolutePath(_root);
	}
	
	public String getAppRoot() {
		return _root;
	}
	
	public CuratorFramework getClient() {
		return _client;
	}
	
	protected FatalExceptionCallback getExHandler() {
		return _exHandler;
	}
	
	public void start() throws Exception {
		_client.start();
	}

	@Override
	public void close() throws IOException {
		_client.close();
	}
	
	public final byte[] read(String path) {
		return read(path, byte[].class);
	}
	
	public final <T> T read(String path, Class<T> expectedType) {
		byte[] data = readBytes(path);
		synchronized(_kryo) {
			return Util.fromKryoBytes(_kryo, data, expectedType);
		}
	}
	
	private byte[] readBytes(String path) {
		byte[] data = null;
		try {
			ensurePathCreated(path);
			data = getClient().getData().forPath(path);
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return data;
	}
	
	public final void waitForData(String path, Object object, long timeout, TimeUnit unit) throws Exception {
		synchronized (_kryo) {			
			ClusterUtil.waitForData(_client, path, Util.toKryoBytes(_kryo, object), timeout, unit);
		}
	}
	
	public final void createOrSetEphemeral(String path, Object object) {
		synchronized (_kryo) {
			createOrSetInternal(path, Util.toKryoBytes(_kryo, object), CreateMode.EPHEMERAL);
		}
	}
	
	public final void createOrSetPersistent(String path, Object object) {
		synchronized (_kryo) {
			createOrSetInternal(path, Util.toKryoBytes(_kryo, object), CreateMode.PERSISTENT);
		}
	}
	
	private void createOrSetInternal(String path, byte[] data, CreateMode createMode) {
		try {
			if (_client.checkExists().forPath(path) == null) {
				_client.create()
					.creatingParentsIfNeeded()
					.withMode(createMode)
					.forPath(path, data);
			} else {
				_client.setData().forPath(path, data);
			}
		} catch (Exception e) {
			_exHandler.signalFatalException(e);
		}
	}
	
	public final void delete(String path) {
		try {
			if (_client.checkExists().forPath(path) != null) {
				_client.delete().forPath(path);
			}
		} catch (Exception e) {
			_exHandler.signalFatalException(e);
		}
	}
	
	public final void deleteChildren(String path) {
		try {
			if (_client.checkExists().forPath(path) != null) {
				List<String> childPaths = _client.getChildren().forPath(path);
				for (String child : childPaths) {
					_client.delete().forPath(ZKPaths.makePath(path, child));
				}
			}
		} catch (Exception e) {
			_exHandler.signalFatalException(e);
		}
	}
	
	public final void ensurePathCreated(String path) {
		ClusterUtil.ensurePathCreated(_client, path, _exHandler);
	}
	
}
