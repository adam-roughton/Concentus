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
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.ClusterPath;
import com.adamroughton.concentus.cluster.ClusterUtil;
import com.adamroughton.concentus.cluster.worker.ClusterServiceStateSignalListener.ListenerDelegate;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInit;
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
	private final FatalExceptionCallback _exCallback;
	private final String _serviceType;
	private final Class<TState> _stateType;
	private final TState _createdState;	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private ClusterServiceStateSignalListener<TState> _listener;
	
	private static class StateChangeEntry<TState extends Enum<TState> & ClusterState> {
		public TState newState;
		public Object stateData;
		public TState expectedState;
	}
	
	private final Disruptor<StateChangeEntry<TState>> _serviceDisruptor;
	private ServiceExecutionContext<?> _serviceContext;
	
	private final ServiceContext<TState> _context = new ServiceContext<TState>() {

		@Override
		public void enterState(final TState newState, final Object stateData, final TState expectedCurrentState) {
			_serviceDisruptor.publishEvent(new EventTranslator<StateChangeEntry<TState>>() {

				@Override
				public void translateTo(StateChangeEntry<TState> event,
						long sequence) {
					event.newState = newState;
					event.stateData = stateData;
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
				_service = _serviceContext.createService(initData.getServiceId(), _context, _deployment);
			}
			
			StateDataHandle dataHandle = new StateDataHandle(event.stateData);
			
			if (event.expectedState != null && event.expectedState != _currentState) {
				return;
			}
			
			// update the service
			Log.info("Entering service state: " + event.newState);
			_service.onStateChanged(event.newState, dataHandle, _cluster);
			Log.info("Entered state: " + event.newState);
			_currentState = event.newState;
			
			// write any data for the coordinator
			String stateDataPath = makeServicePath(SERVICE_STATE_DATA);
			_cluster.createOrSetEphemeral(stateDataPath, dataHandle.getDataForCoordinator());
			
			// confirm that the service is now in the new state, allow coordinator to proceed
			String statePath = makeServicePath(SERVICE_STATE);
			_cluster.createOrSetEphemeral(statePath, event.newState);
		}
		
	};
	
	private final ListenerDelegate<TState> _signalListener = new ListenerDelegate<TState>() {

		@Override
		public void onStateChanged(final TState newState) throws Exception {
			_serviceDisruptor.publishEvent(new EventTranslator<StateChangeEntry<TState>>() {

				@Override
				public void translateTo(StateChangeEntry<TState> event,
						long sequence) {
					// get any state data
					String signalDataPath = makeServicePath(SIGNAL_STATE_DATA);
					Object data = _cluster.read(signalDataPath, Object.class);
					
					event.newState = newState;
					event.stateData = data;
					event.expectedState = null;
				}
			});
		}
	};
	

	
	@SuppressWarnings("unchecked")
	public ServiceContainer(
			ConcentusHandle concentusHandle,
			ClusterHandle clusterHandle,
			ServiceDeployment<TState> serviceDeployment,
			FatalExceptionCallback exCallback) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_cluster = Objects.requireNonNull(clusterHandle);
		_deployment = Objects.requireNonNull(serviceDeployment);
		_exCallback = Objects.requireNonNull(exCallback);
		_serviceType = Objects.requireNonNull(serviceDeployment.serviceType());
		_stateType = Objects.requireNonNull(serviceDeployment.stateType());
		
		_createdState = ClusterUtil.validateStateType(_stateType).getValue0();
		
		_serviceDisruptor = new Disruptor<>(new EventFactory<StateChangeEntry<TState>>() {

			@Override
			public StateChangeEntry<TState> newInstance() {
				return new StateChangeEntry<>();
			}
			
		}, 16, _executor);
		_serviceDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Service State Disruptor", exCallback));
		_serviceDisruptor.handleEventsWith(_serviceStateChangeHandler);
	}

	@SuppressWarnings("unchecked")
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
			_cluster.createOrSetEphemeral(stateTypePath, _stateType);
			
			// write any data for the coordinator
			String stateDataPath = makeServicePath(SERVICE_STATE_DATA);
			_cluster.createOrSetEphemeral(stateDataPath, dataHandle.getDataForCoordinator());
			
			_serviceDisruptor.start();
			
			// create the listener for the signal path (node that the coordinator
			// uses to signal the service to enter a new state)
			String signalPath = makeServicePath(SERVICE_STATE_SIGNAL);
			_listener = new ClusterServiceStateSignalListener<>(
					_stateType,
					_cluster.getClient(), 
					signalPath, 
					_signalListener, 
					_exCallback);
			_listener.start();
			
			// confirm that the service is now created, allow coordinator to proceed
			String statePath = makeServicePath(SERVICE_STATE);
			_cluster.createOrSetEphemeral(statePath, _createdState);
			
			// get component resolver instance
			String componentResolverPath = _cluster.resolvePathFromRoot(COMPONENT_RESOLVER);
			_serviceContext = new ServiceExecutionContext<>(_cluster.read(componentResolverPath, ComponentResolver.class), 
					_cluster, _concentusHandle);
			
		} catch (Exception e) {
			_exCallback.signalFatalException(e);
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
		String serviceTypePath = ZKPaths.makePath(_cluster.resolvePathFromRoot(SERVICES), _serviceType);
		return _cluster.makeIdPath(serviceTypePath);
	}
	
	private String makeServicePath(ClusterPath serviceRelativePath) {
		return serviceRelativePath.getAbsolutePath(serviceRootPath());
	}
	
	/*
	 * Helper classes
	 */
	
	private class StateDataHandle implements StateData<TState> {
		
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
				int serviceId, ServiceContext<TState> serviceContext, ServiceDeployment<TState> deployment) {
			_metricContext.start(serviceId);
			return deployment.createService(serviceId, serviceContext ,_concentusHandle, _metricContext, _componentResolver);
		}
		
	}
	
}
