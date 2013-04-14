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
import com.adamroughton.concentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferReader;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferWriter;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SendRecvSocketReactor;
import com.adamroughton.concentus.messaging.SocketManager;
import com.adamroughton.concentus.messaging.SocketPackage;
import com.adamroughton.concentus.messaging.SocketSettings;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.util.StatefulRunnable;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import static com.adamroughton.concentus.Constants.*;
import static com.adamroughton.concentus.util.Util.*;

public class ClientHandlerService implements ConcentusService {
	
	public final static String SERVICE_TYPE = "ClientHandler";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private ConcentusHandle<? extends Configuration> _concentusHandle;
	
	private final SocketManager _socketManager;
	private final ExecutorService _executor;
	private final Disruptor<byte[]> _recvDisruptor;
	private final Disruptor<byte[]> _routerSendDisruptor;
	private final Disruptor<byte[]> _pubSendDisruptor;
	private final OutgoingEventHeader _outgoingHeader; // both router and pub can share the same header
	private final IncomingEventHeader _incomingHeader; // both router and sub can share the same header
	
	private StatefulRunnable<EventListener> _subListener;
	private SendRecvSocketReactor _routerReactor;
	private ClientHandlerProcessor _processor;
	private Publisher _publisher;
	
	private final int _routerSocketId;
	private final int _subSocketId;
	private final int _pubSocketId;
	
	private int _clientHandlerId;

	public ClientHandlerService(ConcentusHandle<? extends Configuration> concentusHandle) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_socketManager = new SocketManager();
		
		_executor = Executors.newCachedThreadPool();
		
		_recvDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new MultiThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_routerSendDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_pubSendDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_outgoingHeader = new OutgoingEventHeader(0, 2);
		_incomingHeader = new IncomingEventHeader(0, 2);
		
		_recvDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Recv Disruptor", _concentusHandle));
		_routerSendDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Router Send Disruptor", _concentusHandle));
		_pubSendDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Pub Send Disruptor", _concentusHandle));
		
		/*
		 * Configure sockets
		 */
		// router socket
		int routerPort = _concentusHandle.getConfig().getServices().get(SERVICE_TYPE).getPorts().get("input");
		SocketSettings routerSocketSetting = SocketSettings.create()
				.bindToPort(routerPort);
		_routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSetting);
		
		// sub socket
		SocketSettings subSocketSetting = SocketSettings.create()
				.subscribeTo(EventType.STATE_UPDATE);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSetting);

		// pub socket
		_pubSocketId = _socketManager.create(ZMQ.PUB);
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
	
	@SuppressWarnings("unchecked")
	private void onBind(ClusterWorkerHandle cluster) throws Exception {
		// get client handler ID
		byte[] assignment = cluster.getAssignment(SERVICE_TYPE);
		if (assignment.length != 4) 
			throw new RuntimeException(String.format("Expected the assignment to be an Int, " +
					"instead had length %d", assignment.length));
		_clientHandlerId = MessageBytesUtil.readInt(assignment, 0);
		
		_socketManager.bindBoundSockets();
		
		// infrastructure for router socket
		SendQueue<OutgoingEventHeader> routerSendQueue = new SendQueue<>(_outgoingHeader, _routerSendDisruptor);
		SocketPackage routerSocketPackage = _socketManager.createSocketPackage(_routerSocketId);
		SequenceBarrier routerSendBarrier = _routerSendDisruptor.getRingBuffer().newBarrier();
		_routerReactor = new SendRecvSocketReactor(
				new NonBlockingRingBufferWriter<>(_recvDisruptor.getRingBuffer()),
				new NonBlockingRingBufferReader<>(_routerSendDisruptor.getRingBuffer(), routerSendBarrier), 
				_concentusHandle);
		_routerReactor.configure(routerSocketPackage, _outgoingHeader, _incomingHeader);
		
		// infrastructure for sub socket
		SocketPackage subSocketPackage = _socketManager.createSocketPackage(_subSocketId);
		_subListener = Util.asStateful(new EventListener(
				_incomingHeader,
				subSocketPackage, 
				_recvDisruptor.getRingBuffer(), 
				_concentusHandle));
		_socketManager.addDependency(_subSocketId, _subListener);
		

		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader> pubSendQueue = new SendQueue<>(_outgoingHeader, _pubSendDisruptor);
		SocketPackage pubSocketPackage = _socketManager.createSocketPackage(_pubSocketId);
		// event processing infrastructure
		_processor = new ClientHandlerProcessor(
				_concentusHandle.getClock(),
				_clientHandlerId, 
				_routerSocketId,
				_subSocketId,
				routerSendQueue, 
				pubSendQueue, 
				_incomingHeader);
		_publisher = new Publisher(pubSocketPackage, _outgoingHeader);
		
		_recvDisruptor.handleEventsWith(_processor);
		_routerSendDisruptor.handleEventsWith(_routerReactor);
		_pubSendDisruptor.handleEventsWith(_publisher);
		
		// register the service
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _concentusHandle.getNetworkAddress().getHostAddress()));
	}
	
	private void onConnect(ClusterWorkerHandle cluster) throws Exception {
		String[] canonicalStateAddresses = cluster.getAllServices(CanonicalStateService.SERVICE_TYPE);
		if (canonicalStateAddresses.length < 1) {
			throw new RuntimeException("No canonical state services registered!");
		}

		// assuming only one publishes at any given time (i.e. the slave publishes to null)
		int canonicalPubPort = _concentusHandle.getConfig().getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("pub");
		int canonicalSubPort = _concentusHandle.getConfig().getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("sub");
		for (String canonicalStateAddress : canonicalStateAddresses) {
			_socketManager.connectSocket(_subSocketId, String.format("%s:%d", canonicalStateAddress, canonicalPubPort));
			_socketManager.connectSocket(_pubSocketId, String.format("%s:%d", canonicalStateAddress, canonicalSubPort));
		}
	}
	
	private void onStart(ClusterWorkerHandle cluster) throws Exception {
		_recvDisruptor.start();
		_routerSendDisruptor.start();
		_pubSendDisruptor.start();
		_executor.submit(_subListener);
		_executor.submit(_routerReactor);
	}
	
	private void onShutdown(ClusterWorkerHandle cluster) throws Exception {
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
		_socketManager.close();
	}
}
