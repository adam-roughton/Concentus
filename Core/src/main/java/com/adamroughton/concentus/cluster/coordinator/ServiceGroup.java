package com.adamroughton.concentus.cluster.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.ServiceHandleEvent;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.ServiceHandleEvent.EventType;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.ServiceHandleListener;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.util.IdentityWrapper;
import com.adamroughton.concentus.util.TimeoutTracker;
import com.netflix.curator.framework.listen.Listenable;

public class ServiceGroup<TState extends Enum<TState> & ClusterState> implements Iterable<ServiceHandle<TState>> {

	public static class ServiceGroupEvent<TState extends Enum<TState> & ClusterState> {
		
		public enum EventType {
			TIMED_OUT,
			SERVICE_DEATH,
			STATE_READY,
			UPDATE_FAILURE
		}
		
		private final EventType _eventType;
		private final ServiceHandle<TState> _serviceHandle;
		private final TState _state;
		
		public ServiceGroupEvent(EventType eventType, 
				ServiceHandle<TState> serviceHandle,
				TState state) {
			_eventType = eventType;
			_serviceHandle = serviceHandle;
			_state = state;
		}
		
		public EventType getType() {
			return _eventType;
		}
		
		/**
		 * Gets the relevant service handle for {@link EventType#SERVICE_DEATH} events.
		 * Null for all other event types.
		 * @return the service handle if the event is of type {@link EventType#SERVICE_DEATH}
		 */
		public ServiceHandle<TState> getService() {
			return _serviceHandle;
		}
		
		/**
		 * Gets the reference state for {@link EventType#STATE_READY}, {@link EventType#UPDATE_FAILURE}, 
		 * and {@link EventType#TIMED_OUT} events
		 * @return the reference state
		 */
		public TState getState() {
			return _state;
		}
		
	}
	
	public interface ServiceGroupListener<TState extends Enum<TState> & ClusterState> {
		
		void onServiceGroupEvent(ServiceGroup<TState> serviceGroup, ServiceGroupEvent<TState> event);
		
	}
	
	private final AtomicReference<UpdateOperation<TState>> _currentUpdateOperation = new AtomicReference<>(null);
	private final Set<IdentityWrapper<ServiceHandle<TState>>> _services = Collections.newSetFromMap(
			new ConcurrentHashMap<IdentityWrapper<ServiceHandle<TState>>, Boolean>());
	
	private final Executor _timeoutExecutor = Executors.newCachedThreadPool();
	private final Executor _defaultListenerExecutor = Executors.newSingleThreadExecutor();
	private final Executor _serviceListenerExecutor = Executors.newSingleThreadExecutor();
	private final ConcurrentMap<IdentityWrapper<ServiceGroupListener<TState>>, Executor> _listeners = new ConcurrentHashMap<>();
	private Listenable<ServiceGroupListener<TState>> _listenable = new Listenable<ServiceGroupListener<TState>>() {
		
		@Override
		public void removeListener(ServiceGroupListener<TState> listener) {
			_listeners.remove(new IdentityWrapper<>(listener));
		}
		
		@Override
		public void addListener(ServiceGroupListener<TState> listener, Executor executor) {
			Objects.requireNonNull(listener);
			if (executor == null) {
				executor = _defaultListenerExecutor;
			}
			_listeners.put(new IdentityWrapper<>(listener), executor);
		}
		
		@Override
		public void addListener(ServiceGroupListener<TState> listener) {
			addListener(listener, null);
		}
	};
	
	private final ServiceHandleListener<TState> _serviceListener = new ServiceHandleListener<TState>() {

		@Override
		public void onServiceHandleEvent(ServiceHandle<TState> serviceHandle,
				ServiceHandleEvent<TState> event) {
			processServiceEvent(serviceHandle, event.getCurrentState(), event.getEventType());
		}
	};
	
	public interface EnterStateDelegate<TState extends Enum<TState> & ClusterState> {
		/**
		 * Gives the delegate the opportunity to perform the state change, or determine
		 * if one is not needed. Returns whether a stage change was made. 
		 * @param handle the service handle the change should be performed on
		 * @param nextState the state to move to
		 * @return {@code true} if a state change was made, {@code false} otherwise
		 */
		boolean enterState(ServiceHandle<TState> handle, TState nextState);
	}
	
	public void enterStateInBackground(TState state, long timeout, TimeUnit unit) {
		enterStateInBackground(state, new EnterStateDelegate<TState>() {
			
			@Override
			public boolean enterState(ServiceHandle<TState> handle, TState nextState) {
				handle.setState(nextState, null);
				return true;
			}
		}, timeout, unit);
	}
	
	public void enterStateInBackground(final TState state, EnterStateDelegate<TState> delegate, final long timeout, final TimeUnit unit) {	
		HashSet<String> participatingServicePaths = new HashSet<>();
		
		List<ServiceHandle<TState>> services = new ArrayList<>();
		for (IdentityWrapper<ServiceHandle<TState>> serviceWrapper : _services) {
			participatingServicePaths.add(serviceWrapper.get().getServicePath());
			services.add(serviceWrapper.get());
		}
		
		final UpdateOperation<TState> updateOperation = new UpdateOperation<>(state, participatingServicePaths);
		if (!_currentUpdateOperation.compareAndSet(null, updateOperation)) {
			throw new IllegalStateException("The group state is alredy being updated by a previous call to enterState.");
		}
		
		// set states
		for (final ServiceHandle<TState> service : services) {
			if (!delegate.enterState(service, state)) {
				processServiceEvent(service, state, EventType.STATE_CHANGED);
			}
		}
		
		_timeoutExecutor.execute(new Runnable() {

			@Override
			public void run() {
				TimeoutTracker timeoutTracker = new TimeoutTracker(timeout, unit);
				try {
					while (!timeoutTracker.hasTimedOut()) {
						Thread.sleep(timeoutTracker.remainingMillis());
					}
					
					if (_currentUpdateOperation.compareAndSet(updateOperation, null)) {
						_defaultListenerExecutor.execute(new Runnable() {
	
							@Override
							public void run() {
								sendToListeners(new ServiceGroupEvent<>(ServiceGroupEvent.EventType.TIMED_OUT, 
										null, 
										updateOperation.expectedState));
							}
							
						});
					}
				} catch (InterruptedException eInterrupted) {
				}
			}
		});
		
	}
	
	public void waitForStateInBackground(final TState state, final long timeout, final TimeUnit unit) {
		// do the same operation as enterState, except don't actually set the state
		enterStateInBackground(state, new EnterStateDelegate<TState>() {
			
			@Override
			public boolean enterState(ServiceHandle<TState> handle, TState nextState) {
				return false;
			}
		}, timeout, unit);
	}
	
	private void processServiceEvent(ServiceHandle<TState> serviceHandle, TState state, EventType eventType) {
		UpdateOperation<TState> currentOperation = _currentUpdateOperation.get();
		boolean inOperationSet = false;
		boolean operationFinished = false;
		if (currentOperation != null) {
			inOperationSet = currentOperation.participatingServicePaths.contains(serviceHandle.getServicePath());
		}
		
		final ServiceGroupEvent<TState> groupEvent;
		if (eventType == EventType.SERVICE_DEATH) {
			if (inOperationSet) {
				operationFinished = true;
			}
			groupEvent = new ServiceGroupEvent<>(ServiceGroupEvent.EventType.SERVICE_DEATH, serviceHandle, null);
		} else if (eventType == EventType.STATE_CHANGED) {
			TState updatedState = state;
			if (inOperationSet) {
				if (updatedState == currentOperation.expectedState) {
					currentOperation.completedSet.add(serviceHandle.getServicePath());
					if (currentOperation.isComplete()) {
						groupEvent = new ServiceGroupEvent<>(ServiceGroupEvent.EventType.STATE_READY, 
								null, currentOperation.expectedState);
						operationFinished = true;
					} else {
						groupEvent = null;
					}
				} else {
					groupEvent = new ServiceGroupEvent<>(ServiceGroupEvent.EventType.UPDATE_FAILURE, 
							null, currentOperation.expectedState);
					operationFinished = true;
				}
			} else {
				groupEvent = null;
			}
		} else {
			groupEvent = null;
		}
		
		if (operationFinished) {
			_currentUpdateOperation.set(null);
		}
		
		if (groupEvent != null) {
			sendToListeners(groupEvent);
		}
	}
	
	public boolean isUpdatingGroupState() {
		return _currentUpdateOperation.get() != null;
	}
	
	public void addService(ServiceHandle<TState> serviceHandle) {
		_services.add(new IdentityWrapper<>(serviceHandle));
		serviceHandle.getListenable().addListener(_serviceListener, _serviceListenerExecutor);
	}
	
	public void removeService(ServiceHandle<TState> serviceHandle) {
		serviceHandle.getListenable().removeListener(_serviceListener);
		_services.remove(new IdentityWrapper<>(serviceHandle));
	}
	
	public Listenable<ServiceGroupListener<TState>> getListenable() {
		return _listenable;
	}
	
	private void sendToListeners(final ServiceGroupEvent<TState> groupEvent) {
		for (Entry<IdentityWrapper<ServiceGroupListener<TState>>, Executor> entry : _listeners.entrySet()) {
			final ServiceGroupListener<TState> listener = entry.getKey().get();
			Executor executor = entry.getValue();
			executor.execute(new Runnable() {

				@Override
				public void run() {
					listener.onServiceGroupEvent(ServiceGroup.this, groupEvent);
				}
				
			});
		}
	}
	
	@Override
	public Iterator<ServiceHandle<TState>> iterator() {
		return new Iterator<ServiceHandle<TState>> () {

			Iterator<IdentityWrapper<ServiceHandle<TState>>> wrappedIterator = 
					_services.iterator();
			
			@Override
			public boolean hasNext() {
				return wrappedIterator.hasNext();
			}

			@Override
			public ServiceHandle<TState> next() {
				return wrappedIterator.next().get();
			}

			@Override
			public void remove() {
				wrappedIterator.remove();
			}
			
		};
	}
	
	private static class UpdateOperation<TState extends Enum<TState> & ClusterState> {
		
		public final TState expectedState;
		public final Set<String> participatingServicePaths;
		public final Set<String> completedSet = new HashSet<>();
		
		public UpdateOperation(TState expectedState, HashSet<String> participatingServicePaths) {
			this.expectedState = expectedState;
			this.participatingServicePaths = participatingServicePaths;
		}
		
		public boolean isComplete() {
			return participatingServicePaths.size() == completedSet.size();
		}
	}
	
}
