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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusService;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerBridge;
import com.adamroughton.concentus.messaging.MessengerBridge.BridgeDelegate;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.PipelineBranch;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import static com.adamroughton.concentus.Constants.*;

public class ClientHandlerService<TBuffer extends ResizingBuffer> implements ConcentusService {
	
	public final static String SERVICE_TYPE = "ClientHandler";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private final ConcentusHandle<? extends Configuration, TBuffer> _concentusHandle;
	private final MetricContext _metricContext;
	
	private final SocketManager<TBuffer> _socketManager;
	private final ExecutorService _executor;
	
	private final EventQueue<TBuffer> _recvQueue;
	private final EventQueue<TBuffer> _routerSendQueue;
	private final EventQueue<TBuffer> _outQueue;
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
	private EventProcessor _publisher;
	
	private final int _routerSocketId;
	private final int _dealerSetSocketId;
	private final int _subSocketId;
	private final int _pubSocketId;
	
	private int _clientHandlerId;

	public ClientHandlerService(ConcentusHandle<? extends Configuration, TBuffer> concentusHandle, MetricContext metricContext) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = _concentusHandle.newSocketManager();
		
		_executor = Executors.newCachedThreadPool();
		
		Configuration config = concentusHandle.getConfig();
		
		int recvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "recv");
		int routerSendBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerSend");
		int pubBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "pub");
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager.newMessageQueueFactory(_concentusHandle.getEventQueueFactory());
		
		_recvQueue = messageQueueFactory.createMultiProducerQueue(
				"recvQueue",
				recvBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		_routerSendQueue = messageQueueFactory.createSingleProducerQueue("routerSendQueue", 
				routerSendBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		_outQueue = messageQueueFactory.createSingleProducerQueue("pubQueue", 
				pubBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		_outgoingHeader = new OutgoingEventHeader(0, 2);
		_routerRecvHeader = new IncomingEventHeader(0, 2);
		_subRecvHeader = new IncomingEventHeader(0, 1);
		
		/*
		 * Configure sockets
		 */
		// router socket
		int routerPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "input");
		SocketSettings routerSocketSetting = SocketSettings.create()
	/*			.setSupportReliable(true)
				.setReliableBufferLength(2048)
				.setReliableTryAgainMillis(100) */
				.bindToPort(routerPort);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSetting, "input_recv");
		
		SocketSettings dealerSetSocketSettings = SocketSettings.create()
				.setRecvPairAddress(String.format("tcp://%s:%d", 
						concentusHandle.getNetworkAddress().getHostAddress(),
						routerPort));
		_dealerSetSocketId = _socketManager.create(SocketManager.DEALER_SET, dealerSetSocketSettings, "input_send");
		
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
				
//		_routerReactor = new SendRecvMessengerReactor<>(
//				"routerReactor",
//				routerSocketPackageMutex, 
//				_outgoingHeader, 
//				_routerRecvHeader,
//				_recvQueue,
//				_routerSendQueue, 
//				_concentusHandle);
		
		// infrastructure for sub socket
		Mutex<Messenger<TBuffer>> subSocketPackageMutex = _socketManager.getSocketMutex(_subSocketId);
		_subListener = new EventListener<>(
				"updateListener",
				_subRecvHeader,
				subSocketPackageMutex, 
				_recvQueue, 
				_concentusHandle);

		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader, TBuffer> routerSendQueue = new SendQueue<>("processor", _outgoingHeader, _routerSendQueue);
		SendQueue<OutgoingEventHeader, TBuffer> pubSendQueue = new SendQueue<>("processor", _outgoingHeader, _outQueue);
		Mutex<Messenger<TBuffer>> pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		// event processing infrastructure
		_processor = new ClientHandlerProcessor<>(
				_concentusHandle.getClock(),
				_clientHandlerId, 
				_routerSocketId,
				_subSocketId,
				routerSendQueue, 
				pubSendQueue, 
				_routerRecvHeader,
				_subRecvHeader,
				_metricContext);
		_publisher = MessagingUtil.asSocketOwner("publisher", _outQueue, new Publisher<TBuffer>(_outgoingHeader), pubSocketPackageMutex);
		
		// create processing pipeline
		PipelineSection<TBuffer> subRecvSection = ProcessingPipeline.<TBuffer>build(_subListener, _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.asSection();
		PipelineBranch<TBuffer> dealerSetBranch = ProcessingPipeline.<TBuffer>startBranch(_routerSendQueue, _concentusHandle.getClock())
				.then(_dealerSetPublisher)
				.create();
		PipelineBranch<TBuffer> publisherBranch = ProcessingPipeline.<TBuffer>startBranch(_outQueue, _concentusHandle.getClock())
				.then(_publisher)
				.create();
		_pipeline = ProcessingPipeline.<TBuffer>build(_routerListener, _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.join(subRecvSection)
				.into(_recvQueue.createEventProcessor("processor", _processor, _concentusHandle.getClock(), _concentusHandle))
				.thenBranch(dealerSetBranch, publisherBranch)
				.createPipeline(_executor);
		
//		_pipeline = ProcessingPipeline.<TBuffer>startCyclicPipeline(_routerSendQueue, _concentusHandle.getClock())
//				.then(_routerReactor)
//				.thenConnector(_recvQueue)
//				.join(subRecvSection)
//				.into(_recvQueue.createEventProcessor("processor", _processor, _concentusHandle.getClock(), _concentusHandle))
//				.thenConnector(_outQueue)
//				.then(_publisher)
//				.completeCycle(_executor);
		
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
	
	private void onShutdown(ClusterWorkerHandle cluster) throws Exception {
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}
}
