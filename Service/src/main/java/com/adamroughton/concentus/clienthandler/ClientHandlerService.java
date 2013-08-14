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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusService;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.disruptor.EventEntryHandler;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SendRecvMessengerReactor;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import static com.adamroughton.concentus.Constants.*;
import static com.adamroughton.concentus.util.Util.*;

public class ClientHandlerService implements ConcentusService {
	
	public final static String SERVICE_TYPE = "ClientHandler";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private final ConcentusHandle<? extends Configuration> _concentusHandle;
	private final MetricContext _metricContext;
	
	private final SocketManager _socketManager;
	private final ExecutorService _executor;
	
	private final EventQueue<byte[]> _recvQueue;
	private final EventQueue<byte[]> _routerSendQueue;
	private final EventQueue<byte[]> _outQueue;
	private ProcessingPipeline<byte[]> _pipeline;
	
	private final OutgoingEventHeader _outgoingHeader; // both router and pub can share the same header
	private final IncomingEventHeader _incomingHeader; // both router and sub can share the same header
	
	private EventListener _subListener;
	private SendRecvMessengerReactor _routerReactor;
	private ClientHandlerProcessor _processor;
	private EventProcessor _publisher;
	
	private final int _routerSocketId;
	private final int _subSocketId;
	private final int _pubSocketId;
	
	private int _clientHandlerId;

	public ClientHandlerService(ConcentusHandle<? extends Configuration> concentusHandle, MetricContext metricContext) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = _concentusHandle.newSocketManager();
		
		_executor = Executors.newCachedThreadPool();
		
		Configuration config = concentusHandle.getConfig();
		
		int recvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "recv");
		int routerSendBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerSend");
		int pubBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "pub");
		
		EventQueueFactory eventQueueFactory = _concentusHandle.getEventQueueFactory();
		
		_recvQueue = eventQueueFactory.createMultiProducerQueue(
				"recvQueue",
				new EventEntryHandler<byte[]>() {

					@Override
					public byte[] newInstance() {
						return new byte[MSG_BUFFER_ENTRY_LENGTH];
					}

					@Override
					public void clear(byte[] event) {
						MessageBytesUtil.clear(event, 0, event.length);
					}

					@Override
					public void copy(byte[] source, byte[] destination) {
						System.arraycopy(source, 0, destination, 0, source.length);
					}
				}, 
				recvBufferLength, 
				new YieldingWaitStrategy());
		_routerSendQueue = eventQueueFactory.createSingleProducerQueue("routerSendQueue", msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				routerSendBufferLength, 
				new YieldingWaitStrategy());
		_outQueue = eventQueueFactory.createSingleProducerQueue("pubQueue", msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				pubBufferLength, 
				new YieldingWaitStrategy());
		_outgoingHeader = new OutgoingEventHeader(0, 2);
		_incomingHeader = new IncomingEventHeader(0, 2);
		
		/*
		 * Configure sockets
		 */
		// router socket
		int routerPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "input");
		SocketSettings routerSocketSetting = SocketSettings.create()
				.setSupportReliable(true)
				.setReliableBufferLength(2048)
				.setReliableTryAgainMillis(100)
				.bindToPort(routerPort);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSetting, "input");
		
		// sub socket
		SocketSettings subSocketSetting = SocketSettings.create()
				.subscribeTo(EventType.STATE_UPDATE)
				.subscribeTo(EventType.STATE_INFO);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSetting, "sub");

		// pub socket
		_pubSocketId = _socketManager.create(ZMQ.PUB, "pub");
	}

	@Override
	public void onStateChanged(ConcentusServiceState newClusterState,
			ClusterWorkerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		switch (newClusterState) {
			case INIT:
				onInit(cluster);
				break;
			case BIND:
				onBind(cluster);
				break;
			case CONNECT:
				onConnect(cluster);
				break;
			case START:
				onStart(cluster);
				break;
			case SHUTDOWN:
				onShutdown(cluster);
				break;
			default:
		}
		LOG.info("Signalling ready for next state");
		cluster.signalReady();
	}

	@Override
	public Class<ConcentusServiceState> getStateValueClass() {
		return ConcentusServiceState.class;
	}
	
	private void onInit(ClusterWorkerHandle cluster) throws Exception {		
		// Request a client handler ID
		byte[] clientHandlerAssignmentReq = new byte[16];
		MessageBytesUtil.writeUUID(clientHandlerAssignmentReq, 0, cluster.getMyId());
		cluster.requestAssignment(SERVICE_TYPE, clientHandlerAssignmentReq);
	}
	
	private void onBind(ClusterWorkerHandle cluster) throws Exception {
		// get client handler ID
		byte[] assignment = cluster.getAssignment(SERVICE_TYPE);
		if (assignment.length != 4) 
			throw new RuntimeException(String.format("Expected the assignment to be an Int, " +
					"instead had length %d", assignment.length));
		_clientHandlerId = MessageBytesUtil.readInt(assignment, 0);
		
		// infrastructure for router socket
		Mutex<Messenger> routerSocketPackageMutex = _socketManager.getSocketMutex(_routerSocketId);
		_routerReactor = new SendRecvMessengerReactor(
				"routerReactor",
				routerSocketPackageMutex, 
				_outgoingHeader, 
				_incomingHeader,
				_recvQueue,
				_routerSendQueue, 
				_concentusHandle);
		
		// infrastructure for sub socket
		Mutex<Messenger> subSocketPackageMutex = _socketManager.getSocketMutex(_subSocketId);
		_subListener = new EventListener(
				"updateListener",
				_incomingHeader,
				subSocketPackageMutex, 
				_recvQueue, 
				_concentusHandle);

		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader> routerSendQueue = new SendQueue<>("processor", _outgoingHeader, _routerSendQueue);
		SendQueue<OutgoingEventHeader> pubSendQueue = new SendQueue<>("processor", _outgoingHeader, _outQueue);
		Mutex<Messenger> pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		// event processing infrastructure
		_processor = new ClientHandlerProcessor(
				_concentusHandle.getClock(),
				_clientHandlerId, 
				_routerSocketId,
				_subSocketId,
				routerSendQueue, 
				pubSendQueue, 
				_incomingHeader,
				_metricContext);
		_publisher = MessagingUtil.asSocketOwner("publisher", _outQueue, new Publisher(_outgoingHeader), pubSocketPackageMutex);
		
		// create processing pipeline
		PipelineSection<byte[]> subRecvSection = ProcessingPipeline.<byte[]>build(_subListener, _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.asSection();
		_pipeline = ProcessingPipeline.<byte[]>startCyclicPipeline(_routerSendQueue, _concentusHandle.getClock())
				.then(_routerReactor)
				.thenConnector(_recvQueue)
				.join(subRecvSection)
				.into(_recvQueue.createEventProcessor("processor", _processor, _concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_outQueue)
				.then(_publisher)
				.completeCycle(_executor);
		
		// register the service
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _concentusHandle.getNetworkAddress().getHostAddress()));
	}
	
	private void onConnect(ClusterWorkerHandle cluster) throws Exception {
		String[] canonicalStateAddresses = cluster.getAllServices(CanonicalStateService.SERVICE_TYPE);
		if (canonicalStateAddresses.length < 1) {
			throw new RuntimeException("No canonical state services registered!");
		}

		// assuming only one publishes at any given time (i.e. the slave publishes to null)
		int canonicalPubPort = ConfigurationUtil.getPort(_concentusHandle.getConfig(), CanonicalStateService.SERVICE_TYPE ,"pub");
		int canonicalSubPort = ConfigurationUtil.getPort(_concentusHandle.getConfig(), CanonicalStateService.SERVICE_TYPE, "input");
		for (String canonicalStateAddress : canonicalStateAddresses) {
			_socketManager.connectSocket(_subSocketId, String.format("%s:%d", canonicalStateAddress, canonicalPubPort));
			_socketManager.connectSocket(_pubSocketId, String.format("%s:%d", canonicalStateAddress, canonicalSubPort));
		}
	}
	
	private void onStart(ClusterWorkerHandle cluster) throws Exception {
		_pipeline.start();
	}
	
	private void onShutdown(ClusterWorkerHandle cluster) throws Exception {
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}
}
