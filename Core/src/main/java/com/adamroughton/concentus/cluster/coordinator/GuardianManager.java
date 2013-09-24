package com.adamroughton.concentus.cluster.coordinator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.javatuples.Pair;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.VersioningListenable;
import com.adamroughton.concentus.cluster.VersioningListenableContainer;
import com.adamroughton.concentus.cluster.worker.Guardian;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo.ReturnType;
import com.adamroughton.concentus.data.cluster.kryo.GuardianState;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.util.TimeoutTracker;
import com.adamroughton.concentus.util.Util;
import com.adamroughton.concentus.cluster.VersioningListenableContainer.ListenerInvokeDelegate;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;

import static com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.*;

public final class GuardianManager implements Closeable {

	private final CoordinatorClusterHandle _clusterHandle;
	private final ServiceIdAllocator _serviceIdAllocator;
	private final PathChildrenCache _guardianCache;
	
	private enum GuardianTrackerState {
		CREATED,
		READY,
		WAITING,
		RUNNING
	}
	
	private final ConcurrentMap<String, GuardianTrackerState> _guardians = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, GuardianDeployment<?>> _deployments = new ConcurrentHashMap<>();
	private final ConcurrentLinkedQueue<String> _readyHintQueue = new ConcurrentLinkedQueue<>();
	private final String _guardiansRootPath;
	
	private final Kryo _kryo = Util.newKryoInstance();
	
	/*
	 * Called when the state of a guardian is updated
	 */
	private final BackgroundCallback _guardianStateCallback = new BackgroundCallback() {
		
		@Override
		public void processResult(CuratorFramework client, CuratorEvent event)
				throws Exception {
			if (event.getType() == CuratorEventType.GET_DATA) { 
				StateEntry<?> stateEntryObj;
				synchronized(_kryo) {
					stateEntryObj = Util.fromKryoBytes(_kryo, event.getData(), StateEntry.class);
				}
				if (stateEntryObj != null) {
					StateEntry<GuardianState> stateEntry = _clusterHandle.castStateEntry(stateEntryObj, GuardianState.class);
					String guardianPath = ZKPaths.getPathAndNode(event.getPath()).getPath();
					switch (stateEntry.getState()) {
						case CREATED:
							_clusterHandle.setServiceInitData(_serviceIdAllocator, guardianPath, Guardian.SERVICE_TYPE, null);
							_clusterHandle.setServiceSignal(guardianPath, GuardianState.class, GuardianState.READY, null);
							_guardians.put(guardianPath, GuardianTrackerState.CREATED);
							break;
						case READY:
							GuardianDeployment<?> guardianDeployment = _deployments.remove(guardianPath);
							if (guardianDeployment != null) {
								// check if we have return info from the last deployment
								ProcessReturnInfo depRetInfo = stateEntry.getStateData(ProcessReturnInfo.class);
								GuardianDeploymentState deploymentState;
								if (depRetInfo != null) {
									if (depRetInfo.getReturnType() == ReturnType.OK) {
										deploymentState = GuardianDeploymentState.RET_OK;
									} else {
										deploymentState = GuardianDeploymentState.RET_ERROR;
									}
								} else {
									deploymentState = GuardianDeploymentState.RET_OK;
								}
								guardianDeployment.changeState(deploymentState, depRetInfo);
							}
							_guardians.put(guardianPath, GuardianTrackerState.READY);
							_readyHintQueue.add(guardianPath);
							break;
						case RUN:
							_guardians.put(guardianPath, GuardianTrackerState.RUNNING);
						default:
					}
				}
			}
		}
	};
	private final CuratorWatcher _guardianStateWatcher = new CuratorWatcher() {
		
		@Override
		public void process(WatchedEvent event) throws Exception {
			String path = event.getPath();
			if (path != null && 
					path.startsWith(_guardiansRootPath) && 
					event.getType() == EventType.NodeDataChanged) {
				_clusterHandle.getClient().getData()
					.usingWatcher(this)
					.inBackground(_guardianStateCallback)
					.forPath(path);
			}
		}
	};
	private final PathChildrenCacheListener _guardianInstanceListener = new PathChildrenCacheListener() {
		
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
				throws Exception {
			Type eventType = event.getType();
			String guardianPath = event.getData().getPath();
			if (eventType == CHILD_ADDED) {
				// new guardian added
				String statePath = CorePath.SERVICE_STATE.getAbsolutePath(guardianPath);
				_clusterHandle.ensureEphemeralPathCreated(statePath);
				client.getData().usingWatcher(_guardianStateWatcher)
					.inBackground(_guardianStateCallback)
					.forPath(statePath);
			} else if (eventType == CHILD_REMOVED) {
				// guardian no longer available
				_guardians.remove(guardianPath);
				GuardianDeployment<?> deployment = _deployments.remove(guardianPath);
				if (deployment != null) {
					deployment.changeState(GuardianDeploymentState.GUARDIAN_DEATH, null);
				}
			}
		}
	};
	
	
	public GuardianManager(CoordinatorClusterHandle clusterHandle, ServiceIdAllocator serviceIdAllocator) {
		_clusterHandle = Objects.requireNonNull(clusterHandle);
		_serviceIdAllocator = Objects.requireNonNull(serviceIdAllocator);
		
		_guardiansRootPath = ZKPaths.makePath(
				_clusterHandle.resolvePathFromRoot(CorePath.SERVICES), 
				Guardian.SERVICE_TYPE);
		_clusterHandle.ensurePathCreated(_guardiansRootPath);
		_guardianCache = new PathChildrenCache(_clusterHandle.getClient(), _guardiansRootPath, false);
		_guardianCache.getListenable().addListener(_guardianInstanceListener);
	}
	
	public void start() throws Exception {
		_guardianCache.start();
	}
	
	public void close() throws IOException {
		_guardianCache.close();
	}
	
	public <TState extends Enum<TState> & ClusterState> GuardianDeployment<TState> deploy(ServiceDeployment<TState> deployment, 
				ComponentResolver<? extends ResizingBuffer> componentResolver, 
				long timeout, TimeUnit unit) throws InterruptedException, 
			TimeoutException {
		TimeoutTracker timeoutTracker = new TimeoutTracker(timeout, unit);
		boolean deployed = false;
		do {
			String availableGuardian = _readyHintQueue.poll();
			if (availableGuardian != null) {
				if (_guardians.replace(availableGuardian, GuardianTrackerState.READY, GuardianTrackerState.WAITING)) {
					GuardianDeployment<TState> guardianDeployment = 
							new GuardianDeployment<>(availableGuardian, deployment);
					_deployments.put(availableGuardian, guardianDeployment);
					
					// check that the state hasn't been updated in the meantime
					if (_guardians.get(availableGuardian) == GuardianTrackerState.WAITING) {
						_clusterHandle.setServiceSignal(availableGuardian, GuardianState.class, GuardianState.RUN, 
								new Pair<>(deployment, componentResolver));
						return guardianDeployment;
					} else {
						_deployments.remove(availableGuardian, guardianDeployment);
					}
				}
			} else {
				Thread.sleep(Math.min(50, timeoutTracker.remainingMillis()));
			}
		} while (!deployed && !timeoutTracker.hasTimedOut());
		throw new TimeoutException();
	}
	
	public enum GuardianDeploymentState {
		/**
		 * The deployment is running on the guardian (though the service
		 * in the deployment may not be ready for input - check with the
		 * service itself). 
		 */
		RUNNING,
		
		/**
		 * The deployed service exited without error
		 */
		RET_OK,
		
		/**
		 * The deployed service stopped with an error
		 */
		RET_ERROR,
		
		/**
		 * The guardian appears to have stopped
		 */
		GUARDIAN_DEATH
	}
	
	public static interface GuardianDeploymentListener<TState extends Enum<TState> & ClusterState> {
		/**
		 * Called when the state of the deployment changes on the guardian. Return info will only be
		 * provided (i.e. not null) if the state is {@link GuardianDeploymentState#RET_OK} or {@link GuardianDeploymentState#RET_ERROR}.
		 * @param guardianDeployment the deployment package
		 * @param newState the new state of the deployment
		 * @param retInfo return info for the deployed service - only not null if the newState is 
		 * {@link GuardianDeploymentState#RET_OK} or {@link GuardianDeploymentState#RET_ERROR}
		 */
		void onDeploymentChange(GuardianDeployment<TState> guardianDeployment, 
				GuardianDeploymentState newState, 
				ProcessReturnInfo retInfo);
	}
	
	public class GuardianDeployment<TState extends Enum<TState> & ClusterState> {
		
		private final String _guardianPath;
		private final ServiceDeployment<TState> _deployment;
		private final VersioningListenableContainer<GuardianDeploymentStateEntry, GuardianDeploymentListener<TState>> _listenable = 
				new VersioningListenableContainer<>(new ListenerInvokeDelegate<GuardianDeploymentStateEntry, GuardianDeploymentListener<TState>>() {

					@Override
					public void invoke(
							GuardianDeploymentListener<TState> listener,
							GuardianDeploymentStateEntry deploymentStateEntry,
							int updateVersion) {
						GuardianDeploymentState state = deploymentStateEntry.getState();
						ProcessReturnInfo retInfo = deploymentStateEntry.getRetInfo();
						listener.onDeploymentChange(GuardianDeployment.this, state, retInfo);
					}
					
				});
		
		public GuardianDeployment(String guardianPath, 
				ServiceDeployment<TState> deployment) {
			_guardianPath = Objects.requireNonNull(guardianPath);
			_deployment = Objects.requireNonNull(deployment);
		}
		
		private void changeState(GuardianDeploymentState newState, 
				ProcessReturnInfo retInfo) {
			_listenable.newListenerEvent(new GuardianDeploymentStateEntry(newState, retInfo));
		}
		
		public GuardianDeploymentStateEntry getState() {
			return _listenable.getLatestEvent();
		}
		
		public VersioningListenable<GuardianDeploymentListener<TState>> getListenable() {
			return _listenable;
		}
		
		public void stop() {
			// only stop if this current deployment is still active on the guardian
			if (_deployments.remove(_guardianPath, this)) {
				if (_guardians.replace(_guardianPath, GuardianTrackerState.RUNNING, GuardianTrackerState.WAITING)) {
					_clusterHandle.setServiceSignal(_guardianPath, GuardianState.class, GuardianState.READY, null);
				}
			}
		}
		
		public String getGuardianPath() {
			return _guardianPath;
		}
		
		public ServiceDeployment<TState> getDeployment() {
			return _deployment;
		}
		
	}
	
	public static class GuardianDeploymentStateEntry {
		
		private final GuardianDeploymentState _state;
		private final ProcessReturnInfo _retInfo;
		
		public GuardianDeploymentStateEntry(GuardianDeploymentState state, ProcessReturnInfo retInfo) {
			_state = Objects.requireNonNull(state);
			_retInfo = retInfo;
		}
		
		public GuardianDeploymentState getState() {
			return _state;
		}
		
		/**
		 * Return info will only be not null if the state is {@link GuardianDeploymentState#RET_OK} 
		 * or {@link GuardianDeploymentState#RET_ERROR}.
		 * @return the return info from the service last executed on the guardian
		 */
		public ProcessReturnInfo getRetInfo() {
			return _retInfo;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((_retInfo == null) ? 0 : _retInfo.hashCode());
			result = prime * result
					+ ((_state == null) ? 0 : _state.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof GuardianDeploymentStateEntry)) {
				return false;
			}
			GuardianDeploymentStateEntry other = (GuardianDeploymentStateEntry) obj;
			if (_retInfo == null) {
				if (other._retInfo != null) {
					return false;
				}
			} else if (!_retInfo.equals(other._retInfo)) {
				return false;
			}
			if (_state != other._state) {
				return false;
			}
			return true;
		}		
		
	}
	
}
