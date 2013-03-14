package com.adamroughton.consentus.cluster;

import org.apache.zookeeper.KeeperException;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.netflix.curator.framework.CuratorFramework;

public class ClusterUtil {

	public static void ensurePathCreated(
			final CuratorFramework client, 
			final String path, 
			final FatalExceptionCallback exHandler) {
		try {
			ensurePathCreated(client, path);
		} catch (Exception e) {
			exHandler.signalFatalException(e);
		}
	}
	
	public static void ensurePathCreated(
			final CuratorFramework client, 
			final String path) throws Exception {
		if (client.checkExists().forPath(path) == null) {
			try {
				client.create().creatingParentsIfNeeded().forPath(path);
			} catch (KeeperException eKeeper) {
				if (eKeeper.code() != KeeperException.Code.NODEEXISTS) {
					throw eKeeper;
				}
			}
		}
	}
	
}
