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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZKUtil;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.api.UnhandledErrorListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;

public class ClusterParticipant implements Closeable {

	private final ClusterHandleSettings _settings;
	private final CuratorFramework _client;
	private final Kryo _kryo;
		
	public ClusterParticipant(final ClusterHandleSettings handleSettings) {
		_settings = Objects.requireNonNull(handleSettings);
		
		_client = CuratorFrameworkFactory.newClient(handleSettings.zooKeeperAddress(), new ExponentialBackoffRetry(1000, 3));
		_client.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
			
			@Override
			public void unhandledError(String message, Throwable e) {
				handleSettings.exCallback().signalFatalException(e);
			}
		});
		
		_kryo = Util.newKryoInstance();
		if (handleSettings.kryoRegistratorDelegate() != null) {
			handleSettings.kryoRegistratorDelegate().register(_kryo);
		}
	}
	
	public UUID getMyId() {
		return _settings.clusterId();
	}
	
	public String getMyIdString() {
		return getMyId().toString();
	}
	
	public String resolvePathFromRoot(ClusterPath path) {
		if (!path.isRelativeToRoot()) {
			throw new IllegalArgumentException("The path cannot be resolved as it " +
					"is not relative to the application root");
		}
		return path.getAbsolutePath(_settings.zooKeeperAppRoot());
	}
	
	public ClusterHandleSettings settings() {
		return _settings;
	}
	
	public CuratorFramework getClient() {
		return _client;
	}
	
	protected FatalExceptionCallback getExHandler() {
		return _settings.exCallback();
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
	
	public final void waitForData(String path, Object expected, long timeout, TimeUnit unit) throws Exception {
		synchronized (_kryo) {			
			ClusterUtil.waitForData(_client, path, Util.toKryoBytes(_kryo, expected), timeout, unit);
		}
	}
	
	public final byte[] toKryoBytes(Object data) {
		synchronized (_kryo) {
			return Util.toKryoBytes(_kryo, data);
		}
	}
	
	public final <T> T fromKryoBytes(byte[] data, Class<T> expectedType) {
		synchronized(_kryo) {
			return Util.fromKryoBytes(_kryo, data, expectedType);
		}
	}
	
	public final void createOrSetEphemeral(String path, Object object) {
		synchronized (_kryo) {
			createOrSetInternal(path, Util.toKryoBytes(_kryo, object), CreateMode.EPHEMERAL, -1);
		}
	}
	
	public final boolean createOrSetEphemeral(String path, Object object, int expectedVersion) {
		synchronized (_kryo) {
			return createOrSetInternal(path, Util.toKryoBytes(_kryo, object), CreateMode.EPHEMERAL, expectedVersion);
		}
	}
	
	public final void createOrSet(String path, Object object) {
		synchronized (_kryo) {
			createOrSetInternal(path, Util.toKryoBytes(_kryo, object), CreateMode.PERSISTENT, -1);
		}
	}
	
	public final boolean createOrSet(String path, Object object, int expectedVersion) {
		synchronized (_kryo) {
			return createOrSetInternal(path, Util.toKryoBytes(_kryo, object), CreateMode.PERSISTENT, expectedVersion);
		}
	}
	
	private boolean createOrSetInternal(String path, byte[] data, CreateMode createMode, int version) {
		try {
			if (_client.checkExists().forPath(path) == null) {
				if (version != -1 && version != 0) {
					return false;
				}
				_client.create()
					.creatingParentsIfNeeded()
					.withMode(createMode)
					.forPath(path, data);
			} else {
				try {
					_client.setData().withVersion(version).forPath(path, data);
				} catch (KeeperException eKeeper) {
					if (eKeeper.code() == Code.BADVERSION) {
						return false;
					} else {
						throw eKeeper;
					}
				}
			}
			return true;
		} catch (Exception e) {
			_settings.exCallback().signalFatalException(e);
			return false;
		}
	}
	
	public final void delete(String path) {
		try {
			if (_client.checkExists().forPath(path) != null) {
				ZKUtil.deleteRecursive(_client.getZookeeperClient().getZooKeeper(), path);
			}
		} catch (Exception e) {
			_settings.exCallback().signalFatalException(e);
		}
	}
	
	public final void deleteChildren(String path) {
		try {
			if (_client.checkExists().forPath(path) != null) {
				List<String> childPaths = _client.getChildren().forPath(path);
				for (String child : childPaths) {
					delete(ZKPaths.makePath(path, child));
				}
			}
		} catch (Exception e) {
			_settings.exCallback().signalFatalException(e);
		}
	}
	
	public final void ensurePathCreated(String path) {
		ClusterUtil.ensurePathCreated(_client, path, _settings.exCallback());
	}
	
	public final void ensureEphemeralPathCreated(String path) {
		ClusterUtil.ensureEphemeralPathCreated(_client, path, _settings.exCallback());
	}
}
