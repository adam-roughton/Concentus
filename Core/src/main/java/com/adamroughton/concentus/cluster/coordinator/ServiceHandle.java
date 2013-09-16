package com.adamroughton.concentus.cluster.coordinator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.util.IdentityWrapper;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.NodeCache;
import com.netflix.curator.framework.recipes.cache.NodeCacheListener;

public final class ServiceHandle<TState extends Enum<TState> & ClusterState> implements Closeable {
	
	public static class ServiceHandleEvent<TState extends Enum<TState> & ClusterState> {
		
		public enum EventType {
			STATE_CHANGED,
			SERVICE_DEATH
		}
		
		private final EventType _eventType;
		private final TState _currentState;
		private final TState _signalState;
		private final Object _stateData;
		private final Object _signalData;
		
		public ServiceHandleEvent(EventType eventType, TState currentState, TState signalState, 
				Object stateData, Object signalData) {
			_eventType = eventType;
			_currentState = currentState;
			_signalState = signalState;
			_stateData = stateData;
			_signalData = signalData;
		}
		
		public EventType getEventType() {
			return _eventType;
		}
		
		public <TData> TData getStateData(Class<TData> expectedType) {
			return Util.checkedCast(_stateData, expectedType);
		}
		
		public <TData> TData getLastSignalData(Class<TData> expectedType) {
			return Util.checkedCast(_signalData, expectedType);
		}
		
		public TState getLastSignal() {
			return _signalState;
		}
		
		public TState getCurrentState() {
			return _currentState;
		}
	}
	
	public interface ServiceHandleListener<TState extends Enum<TState> & ClusterState> {
		void onServiceHandleEvent(ServiceHandle<TState> serviceHandle, ServiceHandleEvent<TState> event);
	}
	
	private final String _servicePath;
	private final String _serviceType;
	private final Class<TState> _stateType;
	private final CoordinatorClusterHandle _clusterHandle;
	private final Kryo _kryo;
	
	private final Executor _defaultListenerExecutor = Executors.newSingleThreadExecutor();
	private final ConcurrentMap<IdentityWrapper<ServiceHandleListener<TState>>, Executor> _listeners = new ConcurrentHashMap<>();
	private Listenable<ServiceHandleListener<TState>> _listenable = new Listenable<ServiceHandleListener<TState>>() {
		
		@Override
		public void removeListener(ServiceHandleListener<TState> listener) {
			_listeners.remove(new IdentityWrapper<>(listener));
		}
		
		@Override
		public void addListener(ServiceHandleListener<TState> listener, Executor executor) {
			Objects.requireNonNull(listener);
			if (executor == null) {
				executor = _defaultListenerExecutor;
			}
			_listeners.put(new IdentityWrapper<>(listener), executor);
		}
		
		@Override
		public void addListener(ServiceHandleListener<TState> listener) {
			addListener(listener, null);
		}
	};
	
	private final NodeCache _serviceStateCache;
	private final NodeCacheListener _serviceStateListener = new NodeCacheListener() {
		
		private final AtomicInteger _lastSeenVersion = new AtomicInteger(-1);
		private final Kryo _listenerKryo = Util.newKryoInstance();
		
		@Override
		public void nodeChanged() throws Exception {
			final ServiceHandleEvent<TState> event;
			ChildData nodeState = _serviceStateCache.getCurrentData();
			
			if (nodeState == null) {
				// service state node has been deleted; assume service death
				event = new ServiceHandleEvent<TState>(
						ServiceHandleEvent.EventType.SERVICE_DEATH, null, null, null, null);
			} else {
				int version = nodeState.getStat().getVersion();
				if (!tryClaimLatestVersion(version)) return;
				
				TState state = Util.fromKryoBytes(_listenerKryo, nodeState.getData(), _stateType);
				
				// ignore null states (should only happen if nodes are created without state)
				if (state == null) return;
				
				Object stateData = _clusterHandle.read(CorePath.SERVICE_STATE_DATA.getAbsolutePath(_servicePath), Object.class);
				
				TState lastSignal = _clusterHandle.read(CorePath.SERVICE_STATE_SIGNAL.getAbsolutePath(_servicePath), _stateType);
				Object signalData = _clusterHandle.read(CorePath.SIGNAL_STATE_DATA.getAbsolutePath(_servicePath), Object.class);
				
				// ensure that the state hasn't changed in the meantime - if it has, skip this event
				nodeState = _serviceStateCache.getCurrentData();
				if (nodeState == null || nodeState.getStat().getVersion() != version) return;
				
				event = new ServiceHandleEvent<TState>(ServiceHandleEvent.EventType.STATE_CHANGED, 
						state, lastSignal, stateData, signalData);
			}
			
			for (Entry<IdentityWrapper<ServiceHandleListener<TState>>, Executor> entry : _listeners.entrySet()) {
				Executor executor = entry.getValue();
				final ServiceHandleListener<TState> listener = entry.getKey().get();
				executor.execute(new Runnable() {

					@Override
					public void run() {
						listener.onServiceHandleEvent(ServiceHandle.this, event);
					}
					
				});
			}
		}
		
		private boolean tryClaimLatestVersion(int newVersion) {
			boolean retry = false;
			boolean isLatest = false;
			do {
				int currentVersion = _lastSeenVersion.get();
				if (newVersion > currentVersion) {
					retry = _lastSeenVersion.compareAndSet(currentVersion, newVersion);
					isLatest = true;
				} else {
					retry = false;
				}
			} while (retry);
			return isLatest;
		}
	};
	
	public ServiceHandle(String servicePath, 
			String serviceType, 
			Class<TState> stateType,
			CoordinatorClusterHandle clusterHandle) {
		_servicePath = Objects.requireNonNull(servicePath);
		_serviceType = Objects.requireNonNull(serviceType);
		_stateType = Objects.requireNonNull(stateType);
		_clusterHandle = Objects.requireNonNull(clusterHandle);
		_kryo = Util.newKryoInstance();
		_serviceStateCache = new NodeCache(clusterHandle.getClient(), 
				CorePath.SERVICE_STATE.getAbsolutePath(servicePath));
		_serviceStateCache.getListenable().addListener(_serviceStateListener);
	}
	
	public void start() throws Exception {
		_serviceStateCache.start(true);
	}

	@Override
	public void close() throws IOException {
		_serviceStateCache.close();
	}
	
	public Listenable<ServiceHandleListener<TState>> getListenable() {
		return _listenable;
	}
	
	public void setState(TState state, Object stateData) {
		_clusterHandle.signalStateChange(_servicePath, state, stateData);
	}
	
	public String getServicePath() {
		return _servicePath;
	}
	
	public String getServiceType() {
		return _serviceType;
	}
	
	public StateEntry<TState> getCurrentState() {
		boolean hasConsistentView = false;
		StateEntry<TState> entry;
		do {
			ChildData nodeData = _serviceStateCache.getCurrentData();
			if (nodeData == null) {
				entry = new StateEntry<TState>(null, null);
				break;
			}
			
			int version = nodeData.getStat().getVersion();
			
			TState state;
			synchronized (_kryo) {
				state = Util.fromKryoBytes(_kryo, nodeData.getData(), _stateType);
			}
			Object stateData = _clusterHandle.read(CorePath.SERVICE_STATE_DATA.getAbsolutePath(_servicePath), Object.class);
			entry = new StateEntry<TState>(state, stateData);
			
			nodeData = _serviceStateCache.getCurrentData();
			hasConsistentView = nodeData != null && nodeData.getStat().getVersion() == version;
		} while (!hasConsistentView);
		
		return entry;
	}
	
	public static class StateEntry<TState> {
		
		private final TState _state;
		private final Object _stateData;
		
		public StateEntry(TState state, Object stateData) {
			_state = state;
			_stateData = stateData;
		}
		
		public TState getState() {
			return _state;
		}
		
		public <TData> TData getStateData(Class<TData> expectedType) {
			return Util.checkedCast(_stateData, expectedType);
		}
		
	}
}
