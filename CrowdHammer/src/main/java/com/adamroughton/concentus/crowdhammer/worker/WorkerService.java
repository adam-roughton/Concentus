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
package com.adamroughton.concentus.crowdhammer.worker;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.CoreServices;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ClusterService;
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase;
import com.adamroughton.concentus.cluster.worker.ServiceContext;
import com.adamroughton.concentus.cluster.worker.ServiceDeploymentBase;
import com.adamroughton.concentus.cluster.worker.StateData;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
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
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.concentus.util.Util.*;

public final class WorkerService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase {

	public static final ServiceInfo<ServiceState> SERVICE_INFO = new ServiceInfo<>("worker", 
			ServiceState.class, CoreServices.CLIENT_HANDLER.getId());
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final ConcentusHandle _concentusHandle;
	private final MetricContext _metricContext;
	private final int _recvPortRequested;
	private int _recvPort;
	
	private EventQueue<TBuffer> _clientRecvQueue;
	private EventQueue<TBuffer> _clientSendQueue;
	
	private ProcessingPipeline<TBuffer> _pipeline;
	
	private final OutgoingEventHeader _clientSendHeader;
	private final IncomingEventHeader _clientRecvHeader;
	
	private SocketManager<TBuffer> _socketManager;
	private final int _simClientCount;
	private final StructuredArray<Client> _clients;
	
	private SimulatedClientProcessor<TBuffer> _clientProcessor;
	
	private int _routerSocketId;
	private int _dealerSetSocketId;
	
	private EventListener<TBuffer> _routerListener;
	private EventProcessor _dealerSetPublisher;
	
	public static class WorkerServiceDeployment extends ServiceDeploymentBase<ServiceState> {

		private InstanceFactory<? extends ClientAgent> _agentFactory;
		private int _maxClientCount;
		private int _recvPort;
		private int _recvBufferSize;
		private int _sendBufferSize;
		
		// for Kryo
		@SuppressWarnings("unused")
		private WorkerServiceDeployment() {
		}
		
		public WorkerServiceDeployment(
				InstanceFactory<? extends ClientAgent> agentFactory,
				int maxClientCount, int recvPort, 
				int recvBufferSize, int sendBufferSize) {
			super(SERVICE_INFO);
			_agentFactory = Objects.requireNonNull(agentFactory);
			_maxClientCount = maxClientCount;
			_recvPort = recvPort;
			_recvBufferSize = recvBufferSize;
			_sendBufferSize = sendBufferSize;
		}
		
		@Override
		public void onPreStart(StateData stateData) {
			stateData.setDataForCoordinator(_maxClientCount);
		}

		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<ServiceState> createService(
				int serviceId, StateData initData, ServiceContext<ServiceState> context, ConcentusHandle handle,
				MetricContext metricContext, ComponentResolver<TBuffer> resolver) {
			if (!initData.hasData()) {
				throw new RuntimeException("The worker must be provided with a client count to simulate");
			}
			int simClientCount = initData.getData(Integer.class);
			
			if (simClientCount > _maxClientCount)
				throw new IllegalArgumentException(
						String.format("The client count was too large: %d > %d", 
								simClientCount, 
								_maxClientCount));
			
			return new WorkerService<>(_agentFactory, simClientCount, _recvPort, _recvBufferSize, 
					_sendBufferSize, context, handle, metricContext, resolver);
		}
		
	}
	
	public WorkerService(
			final InstanceFactory<? extends ClientAgent> agentFactory,
			int simClientCount,
			int recvPort,
			int recvBufferSize,
			int sendBufferSize,
			ServiceContext<ServiceState> serviceContext,
			ConcentusHandle concentusHandle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver) {
		_simClientCount = simClientCount;
		_recvPortRequested = recvPort;
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		
		_clients = StructuredArray.newInstance(nextPowerOf2(_simClientCount), Client.class, new ComponentFactory<Client>() {

			long index = 0;
			
			@Override
			public Client newInstance(Object[] initArgs) {
				return new Client(index++, _concentusHandle.getClock(), agentFactory.newInstance());
			}
		});
		_clientSendHeader = new OutgoingEventHeader(0, 2);
		_clientRecvHeader = new IncomingEventHeader(0, 2);
		
		_socketManager = resolver.newSocketManager(concentusHandle.getClock());
		
		MessageQueueFactory<TBuffer> messageQueueFactory = 
				_socketManager.newMessageQueueFactory(resolver.getEventQueueFactory());
		
		_clientRecvQueue = messageQueueFactory.createSingleProducerQueue(
				"clientRecvQueue", 
				recvBufferSize,
				Constants.DEFAULT_MSG_BUFFER_SIZE,
				new YieldingWaitStrategy());
		_clientSendQueue = messageQueueFactory.createSingleProducerQueue(
				"clientSendQueue",
				sendBufferSize, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
	}
	
	@Override
	protected void onBind(StateData stateData, ClusterHandle cluster) throws Exception {
		SocketSettings routerSocketSettings = SocketSettings.create()
				.bindToPort(_recvPortRequested);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSettings, "client_recv");
		_recvPort = _socketManager.getBoundPorts(_routerSocketId)[0];
		
		SocketSettings dealerSetSettings = SocketSettings.create()
				.setRecvPairAddress(String.format("tcp://%s:%d",
						_concentusHandle.getNetworkAddress().getHostAddress(),
						_recvPort));
		_dealerSetSocketId = _socketManager.create(SocketManager.DEALER_SET, dealerSetSettings, "client_send");
	}
	
	@Override
	protected void onConnect(StateData stateData, ClusterHandle cluster) throws Exception {
		/*
		 * connect client socket to all client handlers
		 */		
		List<ServiceEndpoint> clientHandlerEndpoints = cluster.getAllServiceEndpoints(
				ConcentusEndpoints.CLIENT_HANDLER.getId());
		final int clientHandlerCount = clientHandlerEndpoints.size();
		for (ServiceEndpoint endpoint : clientHandlerEndpoints) {
			String address = String.format("tcp://%s:%d", endpoint.ipAddress(), endpoint.port());
			_socketManager.connectSocket(_dealerSetSocketId, 
					address);
			Log.info("Connecting to " + address);
		}
		Log.info("Found " + clientHandlerCount + " client handler endpoints");
				
		// process connect events & collect identities
		final byte[][] handlerIds = new byte[clientHandlerCount][];
		Mutex<Messenger<TBuffer>> routerSocketMutex = _socketManager.getSocketMutex(_routerSocketId);
		final Mutex<Messenger<TBuffer>> dealerSetMutex = _socketManager.getSocketMutex(_dealerSetSocketId);
		final TBuffer tmpBuffer = _socketManager.getBufferFactory().newInstance(128);
		routerSocketMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

			@Override
			public void asOwner(final Messenger<TBuffer> routerSocketMessenger) {
				dealerSetMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

					@Override
					public void asOwner(Messenger<TBuffer> dealerSetMessenger) {
						Clock clock = _concentusHandle.getClock();
						long startTime = clock.nanoTime();
						int i = 0;
						while (i < clientHandlerCount && clock.nanoTime() - startTime < TimeUnit.SECONDS.toNanos(60)) {
							routerSocketMessenger.recv(tmpBuffer, _clientRecvHeader, true);
							if (EventHeader.isValid(tmpBuffer, 0)) {
								int identitySegmentMetaData = _clientRecvHeader.getSegmentMetaData(tmpBuffer, 0);
								int identityOffset = EventHeader.getSegmentOffset(identitySegmentMetaData);
								int identityLength = EventHeader.getSegmentLength(identitySegmentMetaData);
								
								handlerIds[i++] = tmpBuffer.readBytes(identityOffset, identityLength);
								
								dealerSetMessenger.send(tmpBuffer, _clientSendHeader, true);
							}
						}
						if (i < clientHandlerCount) {
							throw new RuntimeException("Timed out waiting for client handlers to respond to connection requests");
						}
					}
				});
			}
		});
		
		/*
		 * assign client handler to each client and prepare clients
		 */
		Log.info("Assigning client handlers to clients");
		int nextHandlerIndex = 0;
		for (long clientIndex = 0; clientIndex < _clients.getLength(); clientIndex++) {
			Client client = _clients.get(clientIndex);
			if (clientIndex < _simClientCount) {
				client.setHandlerId(handlerIds[nextHandlerIndex++ % handlerIds.length]);
				//client.setHandlerId(_handlerIds[nextHandlerIndex++ % _handlerIds.length]);
				client.setIsActive(true);
			} else {
				client.setIsActive(false);
			}
		}
				
		// infrastructure for client socket
		_routerListener = new EventListener<>(
				"routerListener",
				_clientRecvHeader,
				routerSocketMutex, 
				_clientRecvQueue,
				_concentusHandle);
		
		_dealerSetPublisher = MessagingUtil.asSocketOwner(
				"dealerSetPub", 
				_clientSendQueue, 
				new Publisher<TBuffer>(_clientSendHeader), 
				dealerSetMutex);

		// event processing infrastructure
		SendQueue<OutgoingEventHeader, TBuffer> clientSendQueue = new SendQueue<>("clientProcessor", _clientSendHeader, _clientSendQueue);
		_clientProcessor = new SimulatedClientProcessor<>(_concentusHandle.getClock(), _clients, _simClientCount, clientSendQueue, _clientRecvHeader, _metricContext);
		
		_pipeline = ProcessingPipeline.<TBuffer>build(_routerListener, _concentusHandle.getClock())
				.thenConnector(_clientRecvQueue)
				.then(_clientRecvQueue.createEventProcessor("clientProcessor", _clientProcessor, _metricContext, 
						_concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_clientSendQueue)
				.then(_dealerSetPublisher)
				.createPipeline(_executor);
	}
	
	@Override
	protected void onStart(StateData stateData, ClusterHandle cluster) throws Exception {
		_pipeline.start();
	}
		
	@Override
	protected void onShutdown(StateData stateData, ClusterHandle cluster) throws Exception {	
		_clientProcessor.stopSendingInput();
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
		
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (client.isActive()) {
				client.reset();
			}
		}
	}
	
}
