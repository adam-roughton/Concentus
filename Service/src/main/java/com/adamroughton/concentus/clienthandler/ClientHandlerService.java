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
import com.adamroughton.concentus.disruptor.DeadlineBasedEventProcessor;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferReader;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferWriter;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SendRecvSocketReactor;
import com.adamroughton.concentus.messaging.SocketManager;
import com.adamroughton.concentus.messaging.SocketMutex;
import com.adamroughton.concentus.messaging.SocketSettings;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.pipeline.PipelineBranch;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import static com.adamroughton.concentus.Constants.*;
import static com.adamroughton.concentus.util.Util.*;

public class ClientHandlerService implements ConcentusService {
	
	public final static String SERVICE_TYPE = "ClientHandler";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private ConcentusHandle<? extends Configuration> _concentusHandle;
	
	private final SocketManager _socketManager;
	private final ExecutorService _executor;
	
	private final RingBuffer<byte[]> _recvBuffer;
	private final RingBuffer<byte[]> _routerSendBuffer;
	private final RingBuffer<byte[]> _pubBuffer;
	private final RingBuffer<byte[]> _metricSendBuffer;
	private ProcessingPipeline<byte[]> _pipeline;
	
	private final OutgoingEventHeader _outgoingHeader; // both router and pub can share the same header
	private final IncomingEventHeader _incomingHeader; // both router and sub can share the same header
	
	private EventListener _subListener;
	private SendRecvSocketReactor _routerReactor;
	private ClientHandlerProcessor _processor;
	private EventProcessor _publisher;
	private EventProcessor _metricPublisher;
	
	private final int _routerSocketId;
	private final int _subSocketId;
	private final int _pubSocketId;
	private final int _metricSocketId;
	
	private int _clientHandlerId;

	public ClientHandlerService(ConcentusHandle<? extends Configuration> concentusHandle) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_socketManager = new SocketManager();
		
		_executor = Executors.newCachedThreadPool();
		
		Configuration config = concentusHandle.getConfig();
		
		int recvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "recv");
		int routerSendBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerSend");
		int pubBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "pub");
		int metricBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "metric");
		
		_recvBuffer = new RingBuffer<>(msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				new MultiThreadedClaimStrategy(recvBufferLength), 
				new YieldingWaitStrategy());
		_routerSendBuffer = new RingBuffer<>(msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				new SingleThreadedClaimStrategy(routerSendBufferLength), 
				new YieldingWaitStrategy());
		_pubBuffer = new RingBuffer<>(msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				new SingleThreadedClaimStrategy(pubBufferLength), 
				new YieldingWaitStrategy());
		_metricSendBuffer = new RingBuffer<>(msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				new SingleThreadedClaimStrategy(metricBufferLength), 
				new BusySpinWaitStrategy());
		_outgoingHeader = new OutgoingEventHeader(0, 2);
		_incomingHeader = new IncomingEventHeader(0, 2);
		
		/*
		 * Configure sockets
		 */
		// router socket
		int routerPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "input");
		SocketSettings routerSocketSetting = SocketSettings.create()
				.bindToPort(routerPort);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSetting);
		
		// sub socket
		SocketSettings subSocketSetting = SocketSettings.create()
				.subscribeTo(EventType.STATE_UPDATE)
				.subscribeTo(EventType.STATE_INFO);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSetting);

		// pub socket
		_pubSocketId = _socketManager.create(ZMQ.PUB);
		
		// metric socket
		int metricPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "pub");
		SocketSettings metricSocketSetting = SocketSettings.create()
				.bindToPort(metricPort);
		_metricSocketId = _socketManager.create(ZMQ.PUB, metricSocketSetting);
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
		
		_socketManager.bindBoundSockets();
		
		// infrastructure for router socket
		SendQueue<OutgoingEventHeader> routerSendQueue = new SendQueue<>(_outgoingHeader, _routerSendBuffer);
		SocketMutex routerSocketPackageMutex = _socketManager.getSocketMutex(_routerSocketId);
		SequenceBarrier routerSendBarrier = _routerSendBuffer.newBarrier();
		_routerReactor = new SendRecvSocketReactor(
				new NonBlockingRingBufferWriter<>(_recvBuffer),
				new NonBlockingRingBufferReader<>(_routerSendBuffer, routerSendBarrier), 
				_concentusHandle);
		_routerReactor.configure(routerSocketPackageMutex, _outgoingHeader, _incomingHeader);
		
		// infrastructure for sub socket
		SocketMutex subSocketPackageMutex = _socketManager.getSocketMutex(_subSocketId);
		_subListener = new EventListener(
				_incomingHeader,
				subSocketPackageMutex, 
				_recvBuffer, 
				_concentusHandle);
		
		// infrastructure for metric socket
		SendQueue<OutgoingEventHeader> metricSendQueue = new SendQueue<>(_outgoingHeader, _metricSendBuffer);
		SocketMutex metricSocketPackageMutex = _socketManager.getSocketMutex(_metricSocketId);
		SequenceBarrier metricSendBarrier = _metricSendBuffer.newBarrier();
		_metricPublisher = MessagingUtil.asSocketOwner(_metricSendBuffer, metricSendBarrier, new Publisher(_outgoingHeader), metricSocketPackageMutex);

		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader> pubSendQueue = new SendQueue<>(_outgoingHeader, _pubBuffer);
		SocketMutex pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		SequenceBarrier pubSendBarrier = _pubBuffer.newBarrier();
		// event processing infrastructure
		_processor = new ClientHandlerProcessor(
				_concentusHandle.getClock(),
				_clientHandlerId, 
				_routerSocketId,
				_subSocketId,
				routerSendQueue, 
				pubSendQueue, 
				metricSendQueue,
				_incomingHeader);
		_publisher = MessagingUtil.asSocketOwner(_pubBuffer, pubSendBarrier,  new Publisher(_outgoingHeader), pubSocketPackageMutex);
		SequenceBarrier processorBarrier = _recvBuffer.newBarrier();
		
		// create processing pipeline
		PipelineBranch<byte[]> metricSendBranch = ProcessingPipeline.startBranch(_metricSendBuffer, _concentusHandle.getClock())
				.then(_metricPublisher)
				.create();
		PipelineBranch<byte[]> pubSendBranch = ProcessingPipeline.startBranch(_pubBuffer, _concentusHandle.getClock())
				.then(_publisher)
				.create();
		PipelineSection<byte[]> subRecvSection = ProcessingPipeline.<byte[]>build(_subListener, _concentusHandle.getClock())
				.thenConnector(_recvBuffer)
				.asSection();
		_pipeline = ProcessingPipeline.<byte[]>startCyclicPipeline(_routerSendBuffer, _concentusHandle.getClock())
				.then(_routerReactor)
				.thenConnector(_recvBuffer)
				.join(subRecvSection)
				.into(new DeadlineBasedEventProcessor<>(
						_concentusHandle.getClock(), _processor, _recvBuffer, processorBarrier, _concentusHandle))
				.attachBranches(metricSendBranch, pubSendBranch)
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
		int canonicalSubPort = ConfigurationUtil.getPort(_concentusHandle.getConfig(), CanonicalStateService.SERVICE_TYPE, "sub");
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
