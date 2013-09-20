package com.adamroughton.concentus.cluster.coordinator;

import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.data.Stat;

import com.adamroughton.concentus.cluster.ClusterHandleSettings;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInit;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.util.TimeoutTracker;

public class CoordinatorClusterHandle extends ClusterHandle {

	private final MetricRegistrationListener _metricRegistrationListener;
	private final Object2IntMap<String> _typeIndexLookup = new Object2IntArrayMap<>();
	
	public CoordinatorClusterHandle(ClusterHandleSettings handleSettings) {
		super(handleSettings);
		_metricRegistrationListener = new MetricRegistrationListener(this);
	}
	
	@Override
	public void start() throws Exception {
		super.start();
		_metricRegistrationListener.start();
	}

	@Override
	public void close() throws IOException {
		_metricRegistrationListener.close();
		super.close();
	}

	public void addMetricRegistrationListener(MetricRegistrationCallback listener) {
		_metricRegistrationListener.addListener(listener);
	}
	
	public boolean removeMetricRegistrationListener(MetricRegistrationCallback listener) {
		return _metricRegistrationListener.removeListener(listener);
	}
	
	public <TState> void waitForState(String servicePath, TState state, long timeout, TimeUnit unit) throws Exception { 
		waitForState(Arrays.asList(servicePath), state, timeout, unit);
	}
	
	public <TState> void waitForState(List<String> servicePaths, TState state, long timeout, TimeUnit unit) throws Exception {
		TimeoutTracker timeoutTracker = new TimeoutTracker(timeout, unit); 
		for (String servicePath : servicePaths) {
			String statePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);
			waitForData(statePath, state, timeoutTracker.getTimeout(), timeoutTracker.getUnit());
		}
	}
	
	public <TState extends Enum<TState> & ClusterState> void setServiceSignal(String servicePath, Class<TState> stateType, TState newState, Object data) {
		try {
			String signalPath = CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(servicePath);
			StateEntry<TState> signalEntry;
			int version;
			do {
				Stat stat = getClient().checkExists().forPath(signalPath);
				if (stat == null) {
					version = -1;
				} else {
					version = stat.getVersion();
				}
				signalEntry = new StateEntry<TState>(stateType, newState, data, version + 1);
			} while (!createOrSet(signalPath, signalEntry, version));
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
	}
	
	public <TState extends Enum<TState> & ClusterState> StateEntry<TState> readServiceState(String servicePath, Class<TState> stateType) throws Exception {
		String serviceStatePath = CorePath.SERVICE_STATE.getAbsolutePath(servicePath);
		return readStateEntry(serviceStatePath, stateType);
	}

	public <TState extends Enum<TState> & ClusterState> StateEntry<TState> readServiceSignal(String servicePath, Class<TState> stateType) throws Exception {
		String serviceStatePath = CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(servicePath);
		return readStateEntry(serviceStatePath, stateType);
	}
	
	private <TState extends Enum<TState> & ClusterState> StateEntry<TState> readStateEntry(String path, Class<TState> stateType) throws Exception {
		return castStateEntry(read(path, StateEntry.class), stateType);
	}
	
	@SuppressWarnings("unchecked")
	public <TState extends Enum<TState> & ClusterState> StateEntry<TState> castStateEntry(StateEntry<?> stateEntryObj, Class<TState> stateType) throws Exception {
		try {
			if (stateEntryObj == null) 
				return new StateEntry<TState>(stateType, null, null, -1);
			
			if (!stateType.equals(stateEntryObj.stateType())) {
				throw new RuntimeException("The state set by the service does not match the expected state (expected " 
						+ stateType.getCanonicalName() + ", got " + stateEntryObj.stateType() + ")");
			}
			return (StateEntry<TState>) stateEntryObj;
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
			return new StateEntry<TState>(stateType, null, null, -1);
		}
	} 
	
	public void setServiceInitData(ServiceIdAllocator serviceIdAllocator, String servicePath, String serviceType, Object initData) {
		int nextIndex;
		synchronized(_typeIndexLookup) {
			if (_typeIndexLookup.containsKey(serviceType)) {
				nextIndex = _typeIndexLookup.getInt(serviceType);
			} else {
				nextIndex = 0;
			}
			_typeIndexLookup.put(serviceType, nextIndex + 1);
		}
		// assign service metric source ID
		int metricSourceId = serviceIdAllocator.newMetricSource(serviceType + nextIndex, serviceType);
		ServiceInit init = new ServiceInit(metricSourceId, initData);
		
		String initDataPath = CorePath.SERVICE_INIT_DATA.getAbsolutePath(servicePath);
		createOrSet(initDataPath, init);
	}
}
