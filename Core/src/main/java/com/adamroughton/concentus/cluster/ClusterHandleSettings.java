package com.adamroughton.concentus.cluster;

import java.util.Objects;
import java.util.UUID;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.util.Util;

public final class ClusterHandleSettings {

	private final String _zooKeeperAddress;
	private final String _zooKeeperAppRoot;
	private final UUID _clusterId;
	private final KryoRegistratorDelegate _kryoRegistratorDelegate;
	private final FatalExceptionCallback _exCallback;
	
	public ClusterHandleSettings(String zooKeeperAddress, 
			String zooKeeperAppRoot,
			FatalExceptionCallback exCallback) {
		this(zooKeeperAddress, zooKeeperAppRoot, UUID.randomUUID(), null, exCallback);
	}
	
	public ClusterHandleSettings(String zooKeeperAddress, 
			String zooKeeperAppRoot,
			UUID clusterId,
			FatalExceptionCallback exCallback) {
		this(zooKeeperAddress, zooKeeperAppRoot, clusterId, null, exCallback);
	}
	
	public ClusterHandleSettings(String zooKeeperAddress, 
			String zooKeeperAppRoot,
			KryoRegistratorDelegate kryoRegistratorDelegate,
			FatalExceptionCallback exCallback) {
		this(zooKeeperAddress, zooKeeperAppRoot, UUID.randomUUID(), kryoRegistratorDelegate, exCallback);
	}
	
	public ClusterHandleSettings(String zooKeeperAddress, 
			String zooKeeperAppRoot,
			UUID clusterId,
			KryoRegistratorDelegate kryoRegistratorDelegate,
			FatalExceptionCallback exCallback) {
		_zooKeeperAddress = Objects.requireNonNull(zooKeeperAddress);
		_zooKeeperAppRoot = Objects.requireNonNull(zooKeeperAppRoot);
		
		if (!Util.isValidZKRoot(zooKeeperAppRoot)) {
			throw new IllegalArgumentException(String.format("The root %s is not a valid ZooKeeper root.", zooKeeperAppRoot));
		}
		
		_clusterId = Objects.requireNonNull(clusterId);
		_kryoRegistratorDelegate = kryoRegistratorDelegate;
		_exCallback = Objects.requireNonNull(exCallback);
	}
	
	public String zooKeeperAddress() {
		return _zooKeeperAddress;
	}
	
	public String zooKeeperAppRoot() {
		return _zooKeeperAppRoot;
	}
	
	public UUID clusterId() {
		return _clusterId;
	}
	
	public KryoRegistratorDelegate kryoRegistratorDelegate() {
		return _kryoRegistratorDelegate;
	}
	
	public FatalExceptionCallback exCallback() {
		return _exCallback;
	}
	
}
