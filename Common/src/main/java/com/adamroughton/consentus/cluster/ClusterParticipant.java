package com.adamroughton.consentus.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

public abstract class ClusterParticipant implements Closeable {

	private final FatalExceptionCallback _exHandler;
	
	private final UUID _clusterParticipantId;
	private final CuratorFramework _client;
	private final String _root;
	
	public ClusterParticipant(
			final String zooKeeperAddress, 
			final String root,
			final FatalExceptionCallback exHandler) {
		_exHandler = Objects.requireNonNull(exHandler);
		_client = CuratorFrameworkFactory.newClient(zooKeeperAddress, new ExponentialBackoffRetry(1000, 3));
		_clusterParticipantId = UUID.randomUUID();
		_root = Objects.requireNonNull(root);
		if (!Util.isValidZKRoot(root)) {
			throw new IllegalArgumentException(String.format("The root %s is not a valid ZooKeeper root.", root));
		}
	}
	
	public UUID getMyId() {
		return _clusterParticipantId;
	}
	
	public String getMyIdString() {
		return String.format("%u", _clusterParticipantId);
	}
	
	protected String getPath(ClusterPath pathType) {
		return pathType.getPath(_root);
	}
	
	protected CuratorFramework getClient() {
		return _client;
	}
	
	protected FatalExceptionCallback getExHandler() {
		return _exHandler;
	}
	
	public void start() throws Exception {
		_client.start();
		
		// ensure base paths are created
		for (ClusterPath path : ClusterPath.values()) {
			ensurePathCreated(path.getPath(_root));
		}
	}

	@Override
	public void close() throws IOException {
		_client.close();
	}
	
	protected final void createOrSetEphemeral(final String path, final byte[] data) {
		try {
			if (_client.checkExists().forPath(path) == null) {
				_client.create().withMode(CreateMode.EPHEMERAL).forPath(path, data);
			} else {
				_client.setData().forPath(path, data);
			}
		} catch (Exception e) {
			_exHandler.signalFatalException(e);
		}
	}
	
	protected final void delete(final String path) {
		try {
			if (_client.checkExists().forPath(path) != null) {
				_client.delete().forPath(path);
			}
		} catch (Exception e) {
			_exHandler.signalFatalException(e);
		}
	}
	
	protected final void ensurePathCreated(String path) {
		ClusterUtil.ensurePathCreated(_client, path, _exHandler);
	}
	
}
