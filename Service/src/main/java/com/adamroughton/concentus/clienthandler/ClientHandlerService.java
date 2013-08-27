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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusService;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.cluster.worker.ClusterListenerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
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
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
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
	private final RoundRobinAllocationStrategy _actionProcAllocationStrategy = new RoundRobinAllocationStrategy();
	
	private final SocketManager<TBuffer> _socketManager;
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
	
	private int _clientHandlerId;

	public ClientHandlerService(ConcentusHandle<? extends Configuration, TBuffer> concentusHandle, MetricContext metricContext) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = _concentusHandle.newSocketManager();
		
		_executor = Executors.newCachedThreadPool();
		
		Configuration config = concentusHandle.getConfig();
		
		int recvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "recv");
		int routerSendBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerSend");
		
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
		_outgoingHeader = new OutgoingEventHeader(0, 2);
		_routerRecvHeader = new IncomingEventHeader(0, 2);
		_subRecvHeader = new IncomingEventHeader(0, 1);
		
		/*
		 * Configure sockets
		 */
		// router socket
		_routerPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "input");
		SocketSettings routerSocketSetting = SocketSettings.create()
				.bindToPort(_routerPort);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSetting, "input_recv");
		
		SocketSettings dealerSetSocketSettings = SocketSettings.create()
				.setRecvPairAddress(String.format("tcp://%s:%d", 
						concentusHandle.getNetworkAddress().getHostAddress(),
						_routerPort));
		_dealerSetSocketId = _socketManager.create(SocketManager.DEALER_SET, dealerSetSocketSettings, "input_send");
		
		// sub socket
		SocketSettings subSocketSetting = SocketSettings.create()
				.subscribeTo(DataType.CANONICAL_STATE_UPDATE);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSetting, "sub");
	}

	@Override
	public void onStateChanged(ConcentusServiceState newClusterState,
			ClusterListenerHandle cluster) throws Exception {
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
	
	private void onInit(ClusterListenerHandle cluster) throws Exception {		
		// Request a client handler ID
		byte[] clientHandlerAssignmentReq = new byte[16];
		BytesUtil.writeUUID(clientHandlerAssignmentReq, 0, cluster.getMyId());
		cluster.requestAssignment(SERVICE_TYPE, clientHandlerAssignmentReq);
	}
	
	private void onBind(ClusterListenerHandle cluster) throws Exception {
		// get client handler ID
		byte[] assignment = cluster.getAssignment(SERVICE_TYPE);
		if (assignment.length != 4) 
			throw new RuntimeException(String.format("Expected the assignment to be an Int, " +
					"instead had length %d", assignment.length));
		_clientHandlerId = BytesUtil.readInt(assignment, 0);
		
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
				.into(_recvQueue.createEventProcessor("processor", _processor, _concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_routerSendQueue)
				.then(_dealerSetPublisher)
				.createPipeline(_executor);
		
		// register the service
		cluster.registerServiceEndpoint(ConcentusEndpoints.CLIENT_HANDLER, 
				_concentusHandle.getNetworkAddress().getHostAddress(),
				_routerPort);
	}
	
	private void onConnect(ClusterListenerHandle cluster) throws Exception {
		String[] canonicalStateAddresses = cluster.getAllServiceEndpoints(ConcentusEndpoints.CANONICAL_STATE_PUB);
		if (canonicalStateAddresses.length < 1) {
			throw new RuntimeException("No canonical state services registered!");
		}
		final String[] actionProcessorAddresses = cluster.getAllServiceEndpoints(ConcentusEndpoints.ACTION_PROCESSOR);
		if (actionProcessorAddresses.length < 1) {
			throw new RuntimeException("No action processor services registered!");
		}
		
		for (String canonicalStateAddress : canonicalStateAddresses) {
			_socketManager.connectSocket(_subSocketId, canonicalStateAddress);
		}
		
		for (String actionProcessorAddress : actionProcessorAddresses) {
			_socketManager.connectSocket(_dealerSetSocketId, actionProcessorAddress);
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
		ArrayList<SocketIdentity> actionProcessorRefs = new ArrayList<>(actionProcessorAddresses.length);
		for (String connectionString : actionProcessorAddresses) {
			actionProcessorRefs.add(_socketManager.resolveIdentity(_dealerSetSocketId, connectionString, 10, TimeUnit.SECONDS));
		}
		_actionProcAllocationStrategy.setActionProcessorRefs(actionProcessorRefs);
	}
	
	private void onStart(ClusterListenerHandle cluster) throws Exception {
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
	
	private void onShutdown(ClusterListenerHandle cluster) throws Exception {
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}
	
}
