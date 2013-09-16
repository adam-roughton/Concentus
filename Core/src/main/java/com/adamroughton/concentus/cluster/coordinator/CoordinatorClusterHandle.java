package com.adamroughton.concentus.cluster.coordinator;

import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInit;
import com.adamroughton.concentus.util.TimeoutTracker;

public class CoordinatorClusterHandle extends ClusterHandle {

	private final MetricRegistrationListener _metricRegistrationListener;
	private final Object2IntMap<String> _typeIndexLookup = new Object2IntArrayMap<>();
	
	public CoordinatorClusterHandle(String zooKeeperAddress, String root,
			UUID clusterId, FatalExceptionCallback exHandler) {
		this(zooKeeperAddress, root, clusterId, 
				KryoRegistratorDelegate.NULL_DELEGATE, exHandler);
	}
	
	public CoordinatorClusterHandle(String zooKeeperAddress, String root,
			UUID clusterId, KryoRegistratorDelegate kryoRegistratorDelegate, FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, kryoRegistratorDelegate, exHandler);
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
	
	public <TState> void signalStateChange(String servicePath, TState newState, Object data) {
		String signalPath = CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(servicePath);
		String signalDataPath = CorePath.SERVICE_STATE_DATA.getAbsolutePath(servicePath);
		
		createOrSetEphemeral(signalDataPath, data);
		createOrSetEphemeral(signalPath, newState);
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
		createOrSetEphemeral(initDataPath, init);
	}
}
