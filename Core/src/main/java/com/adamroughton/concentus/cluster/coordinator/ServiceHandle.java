package com.adamroughton.concentus.cluster.coordinator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.VersioningListenable;
import com.adamroughton.concentus.cluster.VersioningListenableContainer;
import com.adamroughton.concentus.cluster.VersioningListenableContainer.ListenerInvokeDelegate;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.minlog.Log;
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
		private final StateEntry<TState> _serviceStateEntry; 
		private final StateEntry<TState> _signalStateEntry;
		
		public ServiceHandleEvent(EventType eventType, 
				StateEntry<TState> serviceState, 
				StateEntry<TState> signalState) {
			_eventType = eventType;
			_serviceStateEntry = serviceState;
			_signalStateEntry = signalState;
		}
		
		public EventType getEventType() {
			return _eventType;
		}
		
		public <TData> TData getStateData(Class<TData> expectedType) {
			if (_serviceStateEntry == null) return null;
			return _serviceStateEntry.getStateData(expectedType);
		}
		
		public <TData> TData getLastSignalData(Class<TData> expectedType) {
			if (_signalStateEntry == null) return null;
			return _signalStateEntry.getStateData(expectedType);
		}
		
		public TState getLastSignal() {
			if (_signalStateEntry == null) return null;
			return _signalStateEntry.getState();
		}
		
		public TState getCurrentState() {
			if (_serviceStateEntry == null) return null;
			return _serviceStateEntry.getState();
		}
		
		public boolean wasInternalEvent() {
			return _serviceStateEntry != null && _signalStateEntry == null;
		}

		@Override
		public String toString() {
			return "ServiceHandleEvent [eventType=" + _eventType
					+ ", serviceStateEntry=" + _serviceStateEntry
					+ ", signalStateEntry=" + _signalStateEntry + "]";
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
	
	private final VersioningListenableContainer<ServiceHandleEvent<TState>, ServiceHandleListener<TState>> _listenableContainer
		= new VersioningListenableContainer<>(new ListenerInvokeDelegate<ServiceHandleEvent<TState>, ServiceHandleListener<TState>>() {

			@Override
			public void invoke(ServiceHandleListener<TState> listener, ServiceHandleEvent<TState> latestEvent,
					int updateVersion) {
				listener.onServiceHandleEvent(ServiceHandle.this, latestEvent);
			}
		});
	
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
						ServiceHandleEvent.EventType.SERVICE_DEATH, null, null);
			} else {
				int version = nodeState.getStat().getVersion();
				if (!tryClaimLatestVersion(version)) return;
				
				StateEntry<?> stateEntryObj = Util.fromKryoBytes(_listenerKryo, nodeState.getData(), StateEntry.class);
				// ignore null states (should only happen if nodes are created without state)
				if (stateEntryObj == null) return;
				
				StateEntry<TState> stateEntry = _clusterHandle.castStateEntry(stateEntryObj, _stateType);
				StateEntry<TState> lastSignalEntry = null;
				if (stateEntry.version() != -1) {
					lastSignalEntry = _clusterHandle.readServiceSignal(_servicePath, _stateType);
					/* 
					 * ensure that the current signal was the cause of this state entry - if not
					 * we cannot provide a consistent view for this state change: instead, we can
					 * wait for the next state change which should be on its way given the inconsistency
					 */
					if (lastSignalEntry.version() != stateEntry.version()) {
						return;
					}
				}
				event = new ServiceHandleEvent<TState>(ServiceHandleEvent.EventType.STATE_CHANGED, 
						stateEntry, lastSignalEntry);
			}
			Log.info("ServiceHandle.nodeChanged: " + _servicePath + "; " + event.toString());
			
			_listenableContainer.newListenerEvent(event);
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
	
	public void clearFromZooKeeper() throws Exception {
		close();
		_clusterHandle.delete(_servicePath);
	}
	
	public VersioningListenable<ServiceHandleListener<TState>> getListenable() {
		return _listenableContainer;
	}
	
	public void setState(TState state, Object stateData) {
		_clusterHandle.setServiceSignal(_servicePath, _stateType, state, stateData);
	}
	
	public String getServicePath() {
		return _servicePath;
	}
	
	public String getServiceType() {
		return _serviceType;
	}
	
	public StateEntry<TState> getCurrentState() throws Exception {
		ChildData nodeData = _serviceStateCache.getCurrentData();
		if (nodeData == null) {
			return new StateEntry<TState>(_stateType, null, null, -1);
		}
		StateEntry<?> stateEntryObj;
		synchronized (_kryo) {
			stateEntryObj = Util.fromKryoBytes(_kryo, nodeData.getData(), StateEntry.class);
		}
		return _clusterHandle.castStateEntry(stateEntryObj, _stateType);
	}
	
	@Override
	public String toString() {
		return "ServiceHandle [servicePath=" + _servicePath
				+ ", serviceType=" + _serviceType + ", stateType="
				+ _stateType + "]";
	}
}
