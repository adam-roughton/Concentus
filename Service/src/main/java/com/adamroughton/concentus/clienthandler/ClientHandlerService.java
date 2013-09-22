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
package com.adamroughton.concentus.clienthandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.CoreServices;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ClusterService;
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase;
import com.adamroughton.concentus.cluster.worker.ServiceContext;
import com.adamroughton.concentus.cluster.worker.ServiceDeploymentBase;
import com.adamroughton.concentus.cluster.worker.StateData;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerBridge;
import com.adamroughton.concentus.messaging.MessengerBridge.BridgeDelegate;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import static com.adamroughton.concentus.Constants.*;

public class ClientHandlerService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase {
	
	public static class ClientHandlerServiceDeployment extends ServiceDeploymentBase<ServiceState> {

		private int _recvPort;
		private int _recvBufferLength;
		private int _sendBufferLength;
		
		// for Kryo
		@SuppressWarnings("unused")
		private ClientHandlerServiceDeployment() {
		}
		
		public ClientHandlerServiceDeployment(int recvPort, int recvBufferLength, int sendBufferLength) {
			super(new ServiceInfo<>(CoreServices.CLIENT_HANDLER.getId(), 
					ServiceState.class, 
					CoreServices.ACTION_PROCESSOR.getId(), 
					CoreServices.CANONICAL_STATE.getId()));
			_recvPort = recvPort;
			_recvBufferLength = recvBufferLength;
			_sendBufferLength = sendBufferLength;
		}
		
		@Override
		public void onPreStart(StateData stateData) {
		}

		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<ServiceState> createService(
				int serviceId, StateData initData, ServiceContext<ServiceState> context,
				ConcentusHandle handle, MetricContext metricContext,
				ComponentResolver<TBuffer> resolver) {
			return new ClientHandlerService<>(_recvPort, _recvBufferLength, _sendBufferLength, 
					serviceId, handle, metricContext, resolver);
		}
		
	}
	
	private final ConcentusHandle _concentusHandle;
	private final MetricContext _metricContext;
	private final RoundRobinAllocationStrategy _actionProcAllocationStrategy = new RoundRobinAllocationStrategy();
	
	private final ZmqSocketManager<TBuffer> _socketManager;
	private final ExecutorService _executor;
	
	private final EventQueue<TBuffer> _recvQueue;
	private final EventQueue<TBuffer> _routerSendQueue;
	private ProcessingPipeline<TBuffer> _pipeline;
	
	private final OutgoingEventHeader _outgoingHeader; // both router and pub can share the same outgoing header
	private final IncomingEventHeader _routerRecvHeader;
	private final IncomingEventHeader _subRecvHeader;
	
	private Future<?> _routerDealerSetBridgeTask;
	private EventListener<TBuffer> _subListener;
	private EventListener<TBuffer> _routerListener;
	private EventProcessor _dealerSetPublisher;
	//private SendRecvMessengerReactor<TBuffer> _routerReactor;
	private ClientHandlerProcessor<TBuffer> _processor;
	
	private final int _routerPort;
	private final int _routerSocketId;
	private final int _dealerSetSocketId;
	private final int _subSocketId;
	
	private final int _clientHandlerId;

	public ClientHandlerService(
			int recvPort,
			int recvBufferLength,
			int sendBufferLength,
			int serviceId,
			ConcentusHandle concentusHandle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = resolver.newSocketManager(concentusHandle.getClock());
		_clientHandlerId = serviceId;
		
		_executor = Executors.newCachedThreadPool();
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager.newMessageQueueFactory(
				resolver.getEventQueueFactory());
		
		_recvQueue = messageQueueFactory.createMultiProducerQueue(
				"recvQueue",
				recvBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		_routerSendQueue = messageQueueFactory.createSingleProducerQueue("routerSendQueue", 
				sendBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		_outgoingHeader = new OutgoingEventHeader(0, 2);
		_routerRecvHeader = new IncomingEventHeader(0, 2);
		_subRecvHeader = new IncomingEventHeader(0, 1);
		
		/*
		 * Configure sockets
		 */
		// router socket
		SocketSettings routerSocketSetting = SocketSettings.create()
				.bindToPort(recvPort);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSetting, "input_recv");
		int[] boundPorts = _socketManager.getBoundPorts(_routerSocketId);
		_routerPort = boundPorts[0];
		
		SocketSettings dealerSetSocketSettings = SocketSettings.create()
				.setRecvPairAddress(String.format("tcp://%s:%d", 
						concentusHandle.getNetworkAddress().getHostAddress(),
						_routerPort));
		_dealerSetSocketId = _socketManager.create(ZmqSocketManager.DEALER_SET, dealerSetSocketSettings, "input_send");
		
		// sub socket
		SocketSettings subSocketSetting = SocketSettings.create()
				.subscribeTo(DataType.CANONICAL_STATE_UPDATE);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSetting, "sub");
	}
		
	@Override
	protected void onBind(StateData stateData, ClusterHandle cluster) throws Exception {		
		// infrastructure for router socket
		Mutex<Messenger<TBuffer>> routerSocketPackageMutex = _socketManager.getSocketMutex(_routerSocketId);
		_routerListener = new EventListener<>(
				"routerListener", 
				_routerRecvHeader, 
				routerSocketPackageMutex, 
				_recvQueue, 
				_concentusHandle);
		
		Mutex<Messenger<TBuffer>> dealerSetSocketPackageMutex = _socketManager.getSocketMutex(_dealerSetSocketId);
		_dealerSetPublisher = MessagingUtil.asSocketOwner(
				"dealerSetPublisher", 
				_routerSendQueue, 
				new Publisher<TBuffer>(_outgoingHeader), 
				dealerSetSocketPackageMutex);	
		
		// infrastructure for sub socket
		Mutex<Messenger<TBuffer>> subSocketPackageMutex = _socketManager.getSocketMutex(_subSocketId);
		_subListener = new EventListener<>(
				"updateListener",
				_subRecvHeader,
				subSocketPackageMutex, 
				_recvQueue, 
				_concentusHandle);

		SendQueue<OutgoingEventHeader, TBuffer> routerSendQueue = new SendQueue<>("processor", _outgoingHeader, _routerSendQueue);

		// event processing infrastructure
		_processor = new ClientHandlerProcessor<>(
				_actionProcAllocationStrategy,
				_concentusHandle.getClock(), 
				_clientHandlerId, 
				_routerSocketId, 
				_subSocketId, 
				routerSendQueue, 
				_routerRecvHeader, 
				_subRecvHeader, 
				_metricContext);		
		
		// create processing pipeline
		PipelineSection<TBuffer> subRecvSection = ProcessingPipeline.<TBuffer>build(_subListener, _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.asSection();
		_pipeline = ProcessingPipeline.<TBuffer>build(_routerListener, _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.join(subRecvSection)
				.into(_recvQueue.createEventProcessor("processor", _processor, _metricContext, 
						_concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_routerSendQueue)
				.then(_dealerSetPublisher)
				.createPipeline(_executor);
		
		// register the service
		ServiceEndpoint clientHandlerEndpoint = new ServiceEndpoint(ConcentusEndpoints.CLIENT_HANDLER.getId(), 
				_concentusHandle.getNetworkAddress().getHostAddress(), 
				_routerPort);
		cluster.registerServiceEndpoint(clientHandlerEndpoint);
	}
	
	@Override
	protected void onConnect(StateData stateData, ClusterHandle cluster) throws Exception {
		List<ServiceEndpoint> canonicalStateEndpoints = cluster.getAllServiceEndpoints(ConcentusEndpoints.CANONICAL_STATE_PUB.getId());
		if (canonicalStateEndpoints.size() < 1) {
			throw new RuntimeException("No canonical state services registered!");
		}
		List<ServiceEndpoint> actionProcessorEndpoints = cluster.getAllServiceEndpoints(ConcentusEndpoints.ACTION_COLLECTOR.getId());
		if (actionProcessorEndpoints.size() < 1) {
			throw new RuntimeException("No action processor services registered!");
		}
		
		for (ServiceEndpoint endpoint : canonicalStateEndpoints) {
			Log.info("Connecting to canonical state endpoint: " + endpoint);
			_socketManager.connectSocket(_subSocketId, endpoint);
		}
		
		for (ServiceEndpoint endpoint : actionProcessorEndpoints) {
			Log.info("Connecting to action processor endpoint: " + endpoint);
			_socketManager.connectSocket(_dealerSetSocketId, endpoint);
		}
		
		Mutex<Messenger<TBuffer>> routerSocketPackageMutex = _socketManager.getSocketMutex(_routerSocketId);	
		Mutex<Messenger<TBuffer>> dealerSetSocketPackageMutex = _socketManager.getSocketMutex(_dealerSetSocketId);
		
		// create a bridge for router -> dealer set until pipeline is started
		_routerDealerSetBridgeTask = _executor.submit(new MessengerBridge<>(
				new BridgeDelegate<TBuffer>() {

					@Override
					public void onMessageReceived(TBuffer recvBuffer,
							TBuffer sendBuffer,
							Messenger<TBuffer> sendMessenger,
							IncomingEventHeader recvHeader,
							OutgoingEventHeader sendHeader) {
						Log.info(String.format("Forwarding event to dealer set: %s", recvBuffer.toString()));
						
						sendHeader.setIsValid(sendBuffer, recvHeader.isValid(recvBuffer));
						sendHeader.setIsMessagingEvent(sendBuffer, recvHeader.isMessagingEvent(recvBuffer));
						
						int cursor = sendHeader.getEventOffset();
						for (int i = 0; i < recvHeader.getSegmentCount(); i++) {
							int segmentMetaData = recvHeader.getSegmentMetaData(recvBuffer, i);
							int segmentOffset = EventHeader.getSegmentOffset(segmentMetaData);
							int segmentLength = EventHeader.getSegmentLength(segmentMetaData);
							
							recvBuffer.copyTo(sendBuffer, segmentOffset, cursor, segmentLength);
							sendHeader.setSegmentMetaData(sendBuffer, i, cursor, segmentLength);
						}
						sendMessenger.send(sendBuffer, sendHeader, true);
					}
					
				}, 
				Constants.DEFAULT_MSG_BUFFER_SIZE, 
				_socketManager.getBufferFactory(), 
				routerSocketPackageMutex, 
				dealerSetSocketPackageMutex, 
				_routerRecvHeader, 
				_outgoingHeader));
		
		/*
		 * Get the socket identities for all action processors
		 */
		ArrayList<SocketIdentity> actionProcessorRefs = new ArrayList<>(actionProcessorEndpoints.size());
		for (ServiceEndpoint endpoint : actionProcessorEndpoints) {
			actionProcessorRefs.add(_socketManager.resolveIdentity(_dealerSetSocketId, endpoint, 10, TimeUnit.SECONDS));
		}
		_actionProcAllocationStrategy.setActionProcessorRefs(actionProcessorRefs);
	}
	
	@Override
	protected void onStart(StateData stateData, ClusterHandle cluster) throws Exception {
		_routerDealerSetBridgeTask.cancel(true);
		Mutex<Messenger<TBuffer>> routerMutex = _socketManager.getSocketMutex(_routerSocketId);
		routerMutex.waitForRelease(60, TimeUnit.SECONDS);
		if (routerMutex.isOwned()) {
			throw new RuntimeException("Timed out waiting for the messenger bridge to stop");
		} else {
			_routerDealerSetBridgeTask = null;
			_pipeline.start();
		}
	}
	
	@Override
	protected void onShutdown(StateData stateData, ClusterHandle cluster) throws Exception {
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}
	
}
