package com.adamroughton.concentus.cluster;

import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.data.Stat;
import com.adamroughton.concentus.cluster.ClusterUtil.DataComparisonDelegate;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.util.Container;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.google.monitoring.runtime.instrumentation.common.com.google.common.base.Objects;
import com.netflix.curator.framework.CuratorFramework;

public class HelperMethods {

	/*
	 * 
	 * Service State methods
	 * 
	 */
	
	public static <TState extends Enum<TState> & ClusterState> int signalState(CuratorFramework client, Kryo kryo, 
			String servicePath, Class<TState> stateType, TState newState, 
			Object data) throws Exception {	
		String signalPath = CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(servicePath);
		Stat signalStat = client.checkExists().forPath(signalPath);
		int version;
		if (signalStat == null) {
			version = -1;
		} else {
			version = signalStat.getVersion();
		}
		StateEntry<TState> signalEntry = new StateEntry<>(stateType, newState, data, version + 1);
		byte[] signalEntryBytes = Util.toKryoBytes(kryo, signalEntry);
		if (signalStat == null) {
			client.create().creatingParentsIfNeeded().forPath(signalPath, signalEntryBytes);
		} else {
			client.setData().forPath(signalPath, signalEntryBytes);
		}
		return version + 1;
	}
	
	public static <TState extends Enum<TState> & ClusterState> void setServiceState(CuratorFramework client, Kryo kryo, 
			String servicePath, Class<TState> stateType, TState newState, int version,
			Object data) throws Exception {
		String statePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);
		ClusterUtil.ensurePathCreated(client, statePath);
		StateEntry<TState> stateEntry = new StateEntry<TState>(stateType, newState, data, version);
		byte[] stateEntryBytes = Util.toKryoBytes(kryo, stateEntry);
		client.setData().forPath(statePath, stateEntryBytes);
	}
	
	@SuppressWarnings("unchecked")
	public static <TState extends Enum<TState> & ClusterState> StateEntry<TState> getServiceState(CuratorFramework client, Kryo kryo, 
			String servicePath, Class<TState> expectedStateType) throws Exception {
		String statePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);
		ClusterUtil.ensurePathCreated(client, statePath);
		byte[] stateEntryBytes = client.getData().forPath(statePath);
		StateEntry<?> stateEntryObj = Util.fromKryoBytes(kryo, stateEntryBytes, StateEntry.class);
		if (stateEntryObj == null) 
			return null;
		Class<?> actualStateType = stateEntryObj.stateType();
		if (!expectedStateType.equals(actualStateType)) {
			fail("The state type " + actualStateType.getCanonicalName() + " did not match the expected type " 
					+ expectedStateType.getCanonicalName() + ".");
		}
		return (StateEntry<TState>) stateEntryObj;
	}
	
	public static <TState extends Enum<TState> & ClusterState> void waitForState(CuratorFramework client, Kryo kryo, 
			ExceptionCallback exCallback, String servicePath, TState state, 
			long timeout, TimeUnit unit) throws Throwable {
		waitForState(client, kryo, exCallback, servicePath, state, Object.class, timeout, unit);
	}
	
	public static <TState extends Enum<TState> & ClusterState, TData> TData waitForState(CuratorFramework client, final Kryo kryo,
			ExceptionCallback exCallback, String servicePath, TState state, 
			final Class<TData> expectedStateDataType, long timeout, TimeUnit unit) throws Throwable {
		final Container<TData> dataContainer = new Container<>();
		DataComparisonDelegate<TState> compDelegate = new DataComparisonDelegate<TState>() {

			@Override
			public boolean matches(TState expected, byte[] actual) {
				StateEntry<?> stateEntryObj = Util.fromKryoBytes(kryo, actual, StateEntry.class);
				boolean isState = Objects.equal(expected, stateEntryObj.getState());
				if (isState && stateEntryObj != null) {
					dataContainer.set(stateEntryObj.getStateData(expectedStateDataType));
				}
				return isState;
			}
		};
		
		String statePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);

		if (!ClusterUtil.waitForData(client, statePath, state, compDelegate, timeout, unit)) {
			exCallback.throwAnyExceptions();
			fail("Timed out waiting for state to be set");
		}
		return dataContainer.get();
	}
	
}
