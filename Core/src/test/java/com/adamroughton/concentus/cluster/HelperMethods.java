package com.adamroughton.concentus.cluster;

import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.framework.CuratorFramework;

public class HelperMethods {

	/*
	 * 
	 * Service State methods
	 * 
	 */
	
	public static <TState extends Enum<TState> & ClusterState> void signalState(CuratorFramework client, Kryo kryo, 
			String servicePath, TState newState, 
			Object data) throws Exception {
		// add data
		String signalDataPath = CorePath.SIGNAL_STATE_DATA.getAbsolutePath(servicePath);
		byte[] signalDataBytes = Util.toKryoBytes(kryo, data);
		ClusterUtil.ensurePathCreated(client, signalDataPath);
		client.setData().forPath(signalDataPath, signalDataBytes);
		
		// set state
		String signalPath = CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(servicePath);
		ClusterUtil.ensurePathCreated(client, signalPath);
		byte[] stateChangeBytes = Util.toKryoBytes(kryo, newState);
		client.setData().forPath(signalPath, stateChangeBytes);
	}
	
	public static <TState extends Enum<TState> & ClusterState> void setServiceState(CuratorFramework client, Kryo kryo, 
			String servicePath, TState newState, 
			Object data) throws Exception {
		// add data
		String stateDataPath = CorePath.SERVICE_STATE_DATA.getAbsolutePath(servicePath);
		byte[] stateDataBytes = Util.toKryoBytes(kryo, data);
		ClusterUtil.ensurePathCreated(client, stateDataPath);
		client.setData().forPath(stateDataPath, stateDataBytes);
		
		// set state
		String statePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);
		ClusterUtil.ensurePathCreated(client, statePath);
		byte[] stateChangeBytes = Util.toKryoBytes(kryo, newState);
		client.setData().forPath(statePath, stateChangeBytes);
	}
	
	public static <TState extends Enum<TState> & ClusterState> void waitForState(CuratorFramework client, Kryo kryo, 
			ExceptionCallback exCallback, String servicePath, TState state, 
			long timeout, TimeUnit unit) throws Throwable {
		waitForState(client, kryo, exCallback, servicePath, state, Object.class, timeout, unit);
	}
	
	public static <TState extends Enum<TState> & ClusterState, TData> TData waitForState(CuratorFramework client, Kryo kryo,
			ExceptionCallback exCallback, String servicePath, TState state, 
			Class<TData> expectedStateDataType, long timeout, TimeUnit unit) throws Throwable {
		String statePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);
		byte[] stateBytes = Util.toKryoBytes(kryo, state);

		if (!ClusterUtil.waitForData(client, statePath, stateBytes, timeout, unit)) {
			exCallback.throwAnyExceptions();
			fail("Timed out waiting for state to be set");
		}
		
		String stateDataPath = CorePath.SERVICE_STATE_DATA.getAbsolutePath(servicePath);
		return Util.fromKryoBytes(kryo, client.getData().forPath(stateDataPath), expectedStateDataType);
	}
	
}
