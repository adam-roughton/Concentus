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
package com.adamroughton.consentus.clienthandler;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.ConsentusServiceState;
import com.adamroughton.consentus.canonicalstate.CanonicalStateService;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.config.Configuration;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.MessagePartBufferPolicy.NamedOffset;
import com.adamroughton.consentus.messaging.NonblockingEventReceiver;
import com.adamroughton.consentus.messaging.NonblockingEventSender;
import com.adamroughton.consentus.messaging.Publisher;
import com.adamroughton.consentus.messaging.RouterSocketReactor;
import com.adamroughton.consentus.messaging.SocketPackage;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import static com.adamroughton.consentus.Util.*;
import static com.adamroughton.consentus.Constants.*;

public class ClientHandlerService implements ConsentusService {
	
	public final static String SERVICE_TYPE = "ClientHandler";
	
	private Configuration _config;
	private ConsentusProcessCallback _exHandler;
	private InetAddress _networkAddress;
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _recvDisruptor;
	private Disruptor<byte[]> _directSendDisruptor;
	private Disruptor<byte[]> _pubSendDisruptor;
	
	private EventListener _subListener;
	private RouterSocketReactor _routerReactor;
	private ClientHandlerProcessor _processor;
	private Publisher _publisher;
	
	private SocketSettings _subSocketSetting;
	private SocketSettings _routerSocketSetting;
	
	private ZMQ.Socket _subSocket;
	private ZMQ.Socket _routerSocket;
	private ZMQ.Socket _pubSocket;
	
	private ZMQ.Context _zmqContext;
	
	private int _clientHandlerId;

	@Override
	public String name() {
		return String.format("Client Handler %d", _clientHandlerId);
	}

	@Override
	public void onStateChanged(ConsentusServiceState newClusterState,
			Cluster cluster) throws Exception {
		switch (newClusterState) {
			case INIT:
				init(cluster);
				break;
			case BIND:
				bind(cluster);
				break;
			case CONNECT:
				connect(cluster);
				break;
			case START:
				start(cluster);
				break;
			case SHUTDOWN:
				shutdown(cluster);
				break;
		}
		cluster.signalReady();
	}

	@Override
	public Class<ConsentusServiceState> getStateValueClass() {
		return ConsentusServiceState.class;
	}

	@Override
	public void configure(Configuration config,
			ConsentusProcessCallback exHandler, 
			InetAddress networkAddress) {
		_config = config;
		_exHandler = exHandler;
		_networkAddress = networkAddress;
	}
	
	private void init(Cluster cluster) throws Exception {
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		
		_recvDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new MultiThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_directSendDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_pubSendDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		
		// Request a client handler ID
		byte[] clientHandlerAssignmentReq = new byte[16];
		MessageBytesUtil.writeUUID(clientHandlerAssignmentReq, 0, cluster.getMyId());
		cluster.requestAssignment(SERVICE_TYPE, clientHandlerAssignmentReq);
	}
	
	@SuppressWarnings("unchecked")
	private void bind(Cluster cluster) throws Exception {
		// get client handler ID
		byte[] assignment = cluster.getAssignment(SERVICE_TYPE);
		if (assignment.length != 4) 
			throw new RuntimeException(String.format("Expected the assignment to be an Int, " +
					"instead had length %d", assignment.length));
		_clientHandlerId = MessageBytesUtil.readInt(assignment, 0);
		
		_pubSocket = _zmqContext.socket(ZMQ.PUB);
		_subSocket = _zmqContext.socket(ZMQ.SUB);
		_routerSocket = _zmqContext.socket(ZMQ.ROUTER);
		
		int routerPort = _config.getServices().get(SERVICE_TYPE).getPorts().get("input");
		
		_routerSocketSetting = SocketSettings.create()
				.bindToPort(routerPort);
		SocketPackage routerSocketPackage = SocketPackage.create(_routerSocket)
				.setMessageOffsets(new NamedOffset(0, ClientHandlerProcessor.SOCKET_ID_LABEL), 
								   new NamedOffset(16, ClientHandlerProcessor.CONTENT_LABEL));
		
		_subSocketSetting = SocketSettings.create()
				.subscribeTo(EventType.STATE_UPDATE);
		SocketPackage subSocketPackage = SocketPackage.create(_subSocket)
				.setSocketId(ClientHandlerProcessor.SUB_RECV_SOCKET_ID)
				.setMessageOffsets(new NamedOffset(0, ClientHandlerProcessor.SUB_ID_LABEL), // i.e. overwrite
								   new NamedOffset(0, ClientHandlerProcessor.CONTENT_LABEL));

		SocketPackage pubSocketPackage = SocketPackage.create(_pubSocket)
				.setMessageOffsets(0, 0); // send empty first frame
		
		SequenceBarrier directSendBarrier = _directSendDisruptor.getRingBuffer().newBarrier();
		EventProcessingHeader header = new EventProcessingHeader(0, 1);
		
		_subListener = new EventListener(subSocketPackage, 
				_recvDisruptor.getRingBuffer(), 
				_zmqContext, 
				_exHandler);
		_routerReactor = new RouterSocketReactor(routerSocketPackage,
				new NonblockingEventReceiver(_recvDisruptor.getRingBuffer(), header),
				new NonblockingEventSender(_directSendDisruptor.getRingBuffer(), directSendBarrier, header), 
				_exHandler);
		_processor = new ClientHandlerProcessor(_clientHandlerId, 
				_directSendDisruptor.getRingBuffer(), 
				_pubSendDisruptor.getRingBuffer(), 
				header, 
				header, 
				header, 
				routerSocketPackage.getMessagePartPolicy(), 
				subSocketPackage.getMessagePartPolicy(), 
				pubSocketPackage.getMessagePartPolicy());
		_publisher = new Publisher(pubSocketPackage, header);
		
		_recvDisruptor.handleEventsWith(_processor);
		_pubSendDisruptor.handleEventsWith(_publisher);
		
		_routerSocketSetting.configureSocket(_routerSocket);
		_subSocketSetting.configureSocket(_subSocket);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _networkAddress.getHostAddress()));
	}
	
	private void connect(Cluster cluster) throws Exception {
		String[] canonicalStateAddresses = cluster.getAllServices(CanonicalStateService.SERVICE_TYPE);
		if (canonicalStateAddresses.length < 1) {
			throw new RuntimeException("No canonical state services registered!");
		}

		// assuming only one publishes at any given time (i.e. the slave publishes to null)
		int canonicalPubPort = _config.getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("pub");
		int canonicalSubPort = _config.getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("sub");
		for (String canonicalStateAddress : canonicalStateAddresses) {
			_subSocket.connect(String.format("%s:%d", canonicalStateAddress, canonicalPubPort));
			_pubSocket.connect(String.format("%s:%d", canonicalStateAddress, canonicalSubPort));
		}
	}
	
	private void start(Cluster cluster) throws Exception {
		_recvDisruptor.start();
		_pubSendDisruptor.start();
		
		_executor.submit(_subListener);
		_executor.submit(_routerReactor);
	}
	
	private void shutdown(Cluster cluster) throws Exception {
		_zmqContext.term();
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
	}
	
}
