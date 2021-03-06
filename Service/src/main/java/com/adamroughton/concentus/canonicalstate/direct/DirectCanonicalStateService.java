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
package com.adamroughton.concentus.canonicalstate.direct;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.CoreServices;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.actioncollector.ActionCollectorService;
import com.adamroughton.concentus.actioncollector.ActionCollectorService.ActionCollectorServiceDeployment;
import com.adamroughton.concentus.canonicalstate.TickTimerService;
import com.adamroughton.concentus.canonicalstate.TickTimerService.TickTimerServiceDeployment;
import com.adamroughton.concentus.cluster.ClusterHandleSettings;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ClusterService;
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase;
import com.adamroughton.concentus.cluster.worker.ServiceContainerImpl;
import com.adamroughton.concentus.cluster.worker.ServiceContext;
import com.adamroughton.concentus.cluster.worker.ServiceDeploymentBase;
import com.adamroughton.concentus.cluster.worker.StateData;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.zeromq.*;

import static com.adamroughton.concentus.Constants.MSG_BUFFER_ENTRY_LENGTH;

public class DirectCanonicalStateService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase {
	
	public static class DirectCanonicalStateServiceDeployment extends ServiceDeploymentBase<ServiceState> {

		private int _actionCollectorPort;
		private int _actionCollectorTickSubPort;
		private int _actionCollectorRecvBufferLength;
		private int _actionCollectorSendBufferLength;
		private int _pubPort;
		private int _pubBufferLength;
		
		// for Kryo
		@SuppressWarnings("unused")
		private DirectCanonicalStateServiceDeployment() { }
		
		public DirectCanonicalStateServiceDeployment(int actionCollectorPort,
				int actionCollectorTickSubPort,
				int actionCollectorRecvBufferLength,
				int actionCollectorSendBufferLength,
				int pubPort,
				int pubBufferLength) {
			super(new ServiceInfo<>(CoreServices.CANONICAL_STATE.getId(), ServiceState.class),
					ActionCollectorService.SERVICE_INFO, TickTimerService.SERVICE_INFO);
			_actionCollectorPort = actionCollectorPort;
			_actionCollectorTickSubPort = actionCollectorTickSubPort;
			_actionCollectorRecvBufferLength = actionCollectorRecvBufferLength;
			_actionCollectorSendBufferLength = actionCollectorSendBufferLength;
			_pubPort = pubPort;
			_pubBufferLength = pubBufferLength;
		}
		
		@Override
		public void onPreStart(StateData stateData) {
		}

		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<ServiceState> createService(
				int serviceId, StateData initData, ServiceContext<ServiceState> context,
				ConcentusHandle handle, MetricContext metricContext,
				ComponentResolver<TBuffer> resolver) {
			return new DirectCanonicalStateService<>(_actionCollectorPort, _actionCollectorTickSubPort, 
					_actionCollectorRecvBufferLength, _actionCollectorSendBufferLength, _pubPort, 
					_pubBufferLength, serviceId, handle, metricContext, resolver);
		}
		
	}
	
	private final ConcentusHandle _concentusHandle;
	private final MetricContext _metricContext;
	private final ComponentResolver<? extends ResizingBuffer> _componentResolver;
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final ZmqSocketManager<TBuffer> _socketManager;
	private final EventQueue<TBuffer> _outputQueue;
	private final OutgoingEventHeader _pubHeader;
	private final EventQueue<ComputeStateEvent> _recvQueue;
	private CollectiveApplication _application;
	
	private ProcessingPipeline<ComputeStateEvent> _recvPipeline;
	private ProcessingPipeline<TBuffer> _pubPipeline;
	private DirectStateProcessor<TBuffer> _stateProcessor;
	private EventProcessor _publisher;
	
	private ServiceContainerImpl<ServiceState> _actionProcessorContainer;
	
	private ServiceContainerImpl<ServiceState> _tickTimerContainer;
	
	private final int _serviceId;
	private final int _actionCollectorPort;
	private final int _actionCollectorTickSubPort;
	private final int _actionCollectorRecvQueueLength;
	private final int _actionCollectorSendQueueLength;
	private final int _pubPort;
	private final int _pubSocketId;
	
	public DirectCanonicalStateService(
			int actionCollectorPort,
			int actionCollectorTickSubPort,
			int actionCollectorRecvQueueLength,
			int actionCollectorSendQueueLength,
			int pubPort,
			int pubSendQueueLength,
			int serviceId,
			ConcentusHandle concentusHandle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver) {
		_actionCollectorPort = actionCollectorPort;
		_actionCollectorTickSubPort = actionCollectorTickSubPort;
		_actionCollectorRecvQueueLength = actionCollectorRecvQueueLength;
		_actionCollectorSendQueueLength = actionCollectorSendQueueLength;
		
		_serviceId = serviceId;
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_componentResolver = Objects.requireNonNull(resolver);
		
		_socketManager = resolver.newSocketManager(concentusHandle.getClock());

		EventQueueFactory eventQueueFactory = resolver.getEventQueueFactory();
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager.newMessageQueueFactory(eventQueueFactory);
		_outputQueue = messageQueueFactory.createSingleProducerQueue("pubQueue", 
				pubSendQueueLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		
		_recvQueue = eventQueueFactory.createSingleProducerQueue("recvQueue", 
				new EventFactory<ComputeStateEvent>() {

					@Override
					public ComputeStateEvent newInstance() {
						return new ComputeStateEvent();
					}}, 
				4, 
				new YieldingWaitStrategy());
		
		_pubHeader = new OutgoingEventHeader(0, 2);
		
		/*
		 * Configure pub socket
		 */
		SocketSettings pubSocketSettings = SocketSettings.create()
				.bindToPort(pubPort)
				.setHWM(1000);
		_pubSocketId = _socketManager.create(ZMQ.PUB, pubSocketSettings, "pub");
		int[] boundPorts = _socketManager.getBoundPorts(_pubSocketId);
		_pubPort = boundPorts[0];
	}
	
	@Override
	protected void onInit(StateData stateData, ClusterHandle cluster) throws Exception {
		_application = cluster.getApplicationInstanceFactory().newInstance();	
		
		ClusterHandleSettings canonicalStateHandleSettings = cluster.settings();
		String zooKeeperAddress = canonicalStateHandleSettings.zooKeeperAddress();
		String zooKeeperAppRoot = canonicalStateHandleSettings.zooKeeperAppRoot();
		FatalExceptionCallback exCallback = canonicalStateHandleSettings.exCallback();
		
		ClusterHandleSettings actionProcHandleSettings = new ClusterHandleSettings(zooKeeperAddress, 
				zooKeeperAppRoot, exCallback);
		ActionCollectorServiceDeployment actionCollector = new ActionCollectorServiceDeployment(_actionCollectorPort, 
				_actionCollectorTickSubPort, _actionCollectorRecvQueueLength, _actionCollectorSendQueueLength, 
				new DirectTickDelegate<>(_recvQueue), 0, _application.getTickDuration());
		_actionProcessorContainer = new ServiceContainerImpl<>(actionProcHandleSettings, _concentusHandle, actionCollector, _componentResolver);
		_actionProcessorContainer.start();
		
		TickTimerServiceDeployment tickTimerServiceDeployment = new TickTimerServiceDeployment(_application.getTickDuration(), 0);
		ClusterHandleSettings tickTimerHandleSettings = new ClusterHandleSettings(zooKeeperAddress, zooKeeperAppRoot, exCallback);
		_tickTimerContainer = new ServiceContainerImpl<>(tickTimerHandleSettings, _concentusHandle, tickTimerServiceDeployment, _componentResolver);
		_tickTimerContainer.start();
	}
	
	@Override
	protected void onBind(StateData stateData, ClusterHandle cluster) throws Exception {
		// infrastructure for pub socket
		Mutex<Messenger<TBuffer>> pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		_publisher = MessagingUtil.asSocketOwner("publisher", _outputQueue, new Publisher<TBuffer>(_pubHeader), pubSocketPackageMutex);
		
		ServiceEndpoint endpoint = new ServiceEndpoint(_serviceId, ConcentusEndpoints.CANONICAL_STATE_PUB.getId(), 
				_concentusHandle.getNetworkAddress().getHostAddress(),
				_pubPort);
		Log.info("Registering endpoint " + endpoint);
		cluster.registerServiceEndpoint(endpoint);
	}

	@Override
	protected void onStart(StateData stateData, ClusterHandle cluster) {
		SendQueue<OutgoingEventHeader, TBuffer> pubSendQueue = new SendQueue<>("publisher", _pubHeader, _outputQueue);
		
		Clock clock = _concentusHandle.getClock();
		_stateProcessor = new DirectStateProcessor<>(_application, clock, pubSendQueue, _metricContext);		
		
		Runnable startPoint = new Runnable() {
			
			@Override
			public void run() {
			}
		};
		_recvPipeline = ProcessingPipeline.<ComputeStateEvent>build(startPoint, clock)
				.thenConnector(_recvQueue)
				.then(_recvQueue.createEventProcessor("Direct Canonical State Processor", _stateProcessor,
						clock, _concentusHandle))
				.createPipeline(_executor);
		_pubPipeline = ProcessingPipeline.<TBuffer>build(startPoint, clock)
				.thenConnector(_outputQueue)
				.then(_publisher)
				.createPipeline(_executor);
		
		_pubPipeline.start();
		_recvPipeline.start();
	}

	@Override
	protected void onShutdown(StateData stateData, ClusterHandle cluster) throws Exception {
		_actionProcessorContainer.close();
		_recvPipeline.halt(60, TimeUnit.SECONDS);
		_pubPipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}
	
}
