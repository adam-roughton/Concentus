/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.cluster.worker;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.cluster.ClusterHandleSettings;
import com.adamroughton.concentus.cluster.ClusterPath;
import com.adamroughton.concentus.cluster.ClusterUtil;
import com.adamroughton.concentus.cluster.worker.ClusterServiceSignalListener.ListenerDelegate;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInit;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.concentus.metric.eventpublishing.EventPublishingMetricContext;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.CorePath.*;

public final class ServiceContainer<TState extends Enum<TState> & ClusterState> implements Closeable {
	
	private final ConcentusHandle _concentusHandle;
	private final ClusterHandle _cluster;
	private final ServiceDeployment<TState> _deployment;
	private final ComponentResolver<? extends ResizingBuffer> _componentResolver;
	private final ServiceInfo<TState> _serviceInfo;
	private final TState _createdState;	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private ClusterServiceSignalListener<TState> _listener;
	
	private static class StateChangeEntry<TState extends Enum<TState> & ClusterState> {
		public StateEntry<TState> signalEntry;
		public TState expectedState;
	}
	
	private final Disruptor<StateChangeEntry<TState>> _serviceDisruptor;
	private final ServiceExecutionContext<?> _serviceContext;
	
	private final ServiceContext<TState> _context = new ServiceContext<TState>() {

		@Override
		public void enterState(final TState newState, final Object signalData, final TState expectedCurrentState) {
			_serviceDisruptor.publishEvent(new EventTranslator<StateChangeEntry<TState>>() {

				@Override
				public void translateTo(StateChangeEntry<TState> event,
						long sequence) {
					event.signalEntry = new StateEntry<TState>(_serviceInfo.stateType(), newState, signalData, -1);
					event.expectedState = expectedCurrentState;
				}
			});
		}

	};
	private final EventHandler<StateChangeEntry<TState>> _serviceStateChangeHandler = new EventHandler<StateChangeEntry<TState>>() {

		private ClusterService<TState> _service;
		private TState _currentState;
		
		@Override
		public void onEvent(StateChangeEntry<TState> event, long sequence,
				boolean endOfBatch) throws Exception {
			if (_service == null) {
				// initialise the service
				String initDataPath = makeServicePath(SERVICE_INIT_DATA);
				ServiceInit initData = _cluster.read(initDataPath, ServiceInit.class);
				Objects.requireNonNull(initData, "The service must be initialised with a valid " + ServiceInit.class.getName() + " object (was null)");
				StateDataHandle initDataHandle = new StateDataHandle(initData.getDataForService());
				_service = _serviceContext.createService(initData.getServiceId(), initDataHandle, _context, _deployment);
			}
			
			StateDataHandle dataHandle = new StateDataHandle(event.signalEntry.getStateData(Object.class));
			
			if (event.expectedState != null && event.expectedState != _currentState) {
				return;
			}
			
			// update the service
			TState newState = event.signalEntry.getState();
			Log.info("Entering service state: " + newState);
			_service.onStateChanged(newState, dataHandle, _cluster);
			Log.info("Entered state: " + newState);
			_currentState = newState;
			
			// carry the signal version over to the new state
			StateEntry<TState> stateEntry = new StateEntry<TState>(_serviceInfo.stateType(), newState, 
					dataHandle.getDataForCoordinator(), 
					event.signalEntry.version());
						
			// confirm that the service is now in the new state, allow coordinator to proceed
			String statePath = makeServicePath(SERVICE_STATE);
			_cluster.createOrSetEphemeral(statePath, stateEntry);
		}
		
	};
	
	private final ListenerDelegate<TState> _signalListener = new ListenerDelegate<TState>() {

		@Override
		public void onSignalChanged(final StateEntry<TState> newSignalEntry) throws Exception {
			_serviceDisruptor.publishEvent(new EventTranslator<StateChangeEntry<TState>>() {

				@Override
				public void translateTo(StateChangeEntry<TState> event,
						long sequence) {
					event.signalEntry = newSignalEntry;
					event.expectedState = null;
				}
			});
		}
	};
			
	@SuppressWarnings("unchecked")
	public ServiceContainer(
			ClusterHandleSettings clusterHandleSettings,
			ConcentusHandle concentusHandle,
			ServiceDeployment<TState> serviceDeployment,
			ComponentResolver<? extends ResizingBuffer> componentResolver) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_cluster = new ClusterHandle(clusterHandleSettings);
		_deployment = Objects.requireNonNull(serviceDeployment);
		_componentResolver = Objects.requireNonNull(componentResolver);
		_serviceInfo = Objects.requireNonNull(_deployment.serviceInfo());
		
		_createdState = ClusterUtil.validateStateType(_serviceInfo.stateType()).getValue0();
		
		_serviceContext = new ServiceExecutionContext<>(_componentResolver, 
				_cluster, _concentusHandle);
		
		_serviceDisruptor = new Disruptor<>(new EventFactory<StateChangeEntry<TState>>() {

			@Override
			public StateChangeEntry<TState> newInstance() {
				return new StateChangeEntry<>();
			}
			
		}, 16, _executor);
		_serviceDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Service State Disruptor", _concentusHandle));
		_serviceDisruptor.handleEventsWith(_serviceStateChangeHandler);
	}

	public void start() {
		/*
		 * Register the service with the coordinator.
		 */
		try {
			_cluster.start();
			
			StateDataHandle dataHandle = new StateDataHandle(null);
			_deployment.onPreStart(dataHandle);
			
			// ensure service paths are created
			_cluster.ensurePathCreated(serviceRootPath());
			
			// write the expected state type
			String stateTypePath = makeServicePath(SERVICE_STATE_TYPE);
			_cluster.createOrSet(stateTypePath, _serviceInfo.stateType());
			
			_serviceDisruptor.start();
			
			// create the listener for the signal path (node that the coordinator
			// uses to signal the service to enter a new state)
			String signalPath = makeServicePath(SERVICE_STATE_SIGNAL);
			_listener = new ClusterServiceSignalListener<>(
					_serviceInfo.stateType(),
					_cluster.getClient(), 
					signalPath, 
					_signalListener,
					_concentusHandle);
			_listener.start();
			
			// confirm that the service is now created, allow coordinator to proceed
			StateEntry<TState> stateEntry = new StateEntry<TState>(_serviceInfo.stateType(), _createdState, 
					dataHandle.getDataForCoordinator(), -1);			
			String statePath = makeServicePath(SERVICE_STATE);
			_cluster.createOrSetEphemeral(statePath, stateEntry);
			Log.info(_serviceInfo.serviceType() + " registered with cluster: " + serviceRootPath());
		} catch (Exception e) {
			_concentusHandle.signalFatalException(e);
		}
	}
	
	public void close() {
		if (_listener != null) {
			try {
				_listener.close();
			} catch (IOException eIO) {
			}
			_listener = null;
		}
		_serviceDisruptor.shutdown();
		_cluster.delete(serviceRootPath());
	}
	
	private String serviceRootPath() {
		String serviceTypePath = ZKPaths.makePath(_cluster.resolvePathFromRoot(SERVICES), _serviceInfo.serviceType());
		return _cluster.makeIdPath(serviceTypePath);
	}
	
	private String makeServicePath(ClusterPath serviceRelativePath) {
		return serviceRelativePath.getAbsolutePath(serviceRootPath());
	}
	
	/*
	 * Helper classes
	 */
	
	private class StateDataHandle implements StateData {
		
		private final Object _data;
		private Object _dataForCoordinator;
		
		public StateDataHandle(Object data) {
			_data = data;
		}
		
		@Override
		public boolean hasData() {
			return _data != null;
		}

		@Override
		public <T> T getData(Class<T> expectedType) {
			return Util.checkedCast(_data, expectedType);
		}

		@Override
		public <T> void setDataForCoordinator(T data) {
			_dataForCoordinator = data;
		}
		
		public Object getDataForCoordinator() {
			return _dataForCoordinator;
		}
		
	}
	
	private static class ServiceExecutionContext<TBuffer extends ResizingBuffer> {
		
		private final EventPublishingMetricContext<TBuffer> _metricContext;
		private final ComponentResolver<TBuffer> _componentResolver;
		private final ConcentusHandle _concentusHandle;
		
		public ServiceExecutionContext(ComponentResolver<TBuffer> componentResolver, 
				ClusterHandle clusterHandle,
				ConcentusHandle concentusHandle) {
			_componentResolver = Objects.requireNonNull(componentResolver);
			_concentusHandle = Objects.requireNonNull(concentusHandle);
			_metricContext = new EventPublishingMetricContext<>(clusterHandle, Constants.METRIC_TICK, 
					TimeUnit.SECONDS.toMillis(Constants.METRIC_BUFFER_SECONDS), _concentusHandle.getClock(), componentResolver);
		}
		
		public <TState extends Enum<TState> & ClusterState> ClusterService<TState> createService(
				int serviceId, StateData initData, ServiceContext<TState> serviceContext, ServiceDeployment<TState> deployment) {
			_metricContext.start(serviceId);
			return deployment.createService(serviceId, initData, serviceContext ,_concentusHandle, _metricContext, _componentResolver);
		}
		
	}
	
}
