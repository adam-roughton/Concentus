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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.clienthandler.ClientHandlerService;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.crowdhammer.CrowdHammerService;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.crowdhammer.metriclistener.MetricListenerService;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventProcessor;
import com.adamroughton.concentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferReader;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferWriter;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MultiSocketOutgoingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SendRecvSocketReactor;
import com.adamroughton.concentus.messaging.SocketManager;
import com.adamroughton.concentus.messaging.SocketPackage;
import com.adamroughton.concentus.messaging.SocketPollInSet;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.concentus.util.Util.*;

public final class WorkerService implements CrowdHammerService {

	public static final String SERVICE_TYPE = "CrowdHammerWorker";
	private static final Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final ConcentusHandle<? extends CrowdHammerConfiguration> _concentusHandle;
	
	private final Disruptor<byte[]> _clientRecvDisruptor;
	private final Disruptor<byte[]> _clientSendDisruptor;
	private final Disruptor<byte[]> _metricSendDisruptor;
	
	private final MultiSocketOutgoingEventHeader _clientSendHeader;
	private final IncomingEventHeader _clientRecvHeader;
	private final OutgoingEventHeader _metricSendHeader;
	
	private final SocketManager _socketManager = new SocketManager();
	private final int _metricPubSocketId;
	private final int _maxClients;
	private final StructuredArray<Client> _clients;
	
	private int _clientCountForTest;
	private SimulatedClientProcessor _clientProcessor;
	private Publisher _metricPublisher;
	
	private SendRecvSocketReactor _clientSocketReactor;
	private int[] _handlerIds;
	
	public WorkerService(ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle, int maxClientCount) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_maxClients = maxClientCount;
		_clients = StructuredArray.newInstance(nextPowerOf2(_maxClients), Client.class, new Class<?>[] { Clock.class }, _concentusHandle.getClock());
		
		_clientRecvDisruptor = new Disruptor<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_clientSendDisruptor = new Disruptor<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		_metricSendDisruptor = new Disruptor<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		
		_clientSendHeader = new MultiSocketOutgoingEventHeader(0, 1);
		_clientRecvHeader = new IncomingEventHeader(0, 1);
		_metricSendHeader = new OutgoingEventHeader(0, 2);
		
		_clientRecvDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Client Recv Disruptor", _concentusHandle));
		_clientSendDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Client Send Disruptor", _concentusHandle));
		_metricSendDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Metric Send Disruptor", _concentusHandle));
		
		/*
		 * Configure fixed sockets
		 */

		// metric socket
		_metricPubSocketId = _socketManager.create(ZMQ.PUB);
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			ClusterWorkerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		if (newClusterState == CrowdHammerServiceState.INIT) {
			init(cluster);
		} else if (newClusterState == CrowdHammerServiceState.CONNECT) {
			connect(cluster);
		} else if (newClusterState == CrowdHammerServiceState.INIT_TEST) {
			initTest(cluster);
		} else if (newClusterState == CrowdHammerServiceState.SET_UP_TEST) {
			setUpTest(cluster);
		} else if (newClusterState == CrowdHammerServiceState.START_SUT) {
			startSUT(cluster);
		} else if (newClusterState == CrowdHammerServiceState.EXEC_TEST) {
			executeTest(cluster);
		} else if (newClusterState == CrowdHammerServiceState.STOP_SENDING_EVENTS) {
			stopSendingInputEvents(cluster);
		} else if (newClusterState == CrowdHammerServiceState.TEAR_DOWN) {
			teardown(cluster);
		} else if (newClusterState == CrowdHammerServiceState.SHUTDOWN) {
			shutdown(cluster);
		}
		LOG.info("Signalling ready for next state");
		cluster.signalReady();
	}

	@Override
	public Class<CrowdHammerServiceState> getStateValueClass() {
		return CrowdHammerServiceState.class;
	}
	
	@SuppressWarnings("unchecked")
	private void init(ClusterWorkerHandle cluster) throws Exception {
		_socketManager.bindBoundSockets();
		
		// infrastructure for client socket
		SequenceBarrier clientSendBarrier = _clientSendDisruptor.getRingBuffer().newBarrier();
		SendQueue<MultiSocketOutgoingEventHeader> clientSendQueue = new SendQueue<>(_clientSendHeader, _clientSendDisruptor);
		_clientSocketReactor = new SendRecvSocketReactor(
				new NonBlockingRingBufferWriter<>(_clientRecvDisruptor.getRingBuffer()),
				new NonBlockingRingBufferReader<>(_clientSendDisruptor.getRingBuffer(), clientSendBarrier), 
				_concentusHandle);

		// infrastructure for metric socket
		SendQueue<OutgoingEventHeader> metricSendQueue = new SendQueue<>(_metricSendHeader, _metricSendDisruptor);
		SocketPackage pubSocketPackage = _socketManager.createSocketPackage(_metricPubSocketId);
		
		// event processing infrastructure
		SequenceBarrier recvBarrier = _clientRecvDisruptor.getRingBuffer().newBarrier();
		_clientProcessor = new SimulatedClientProcessor(_concentusHandle.getClock(), _clients, clientSendQueue, metricSendQueue, _clientRecvHeader);
		_metricPublisher = new Publisher(pubSocketPackage, _metricSendHeader);
		
		_clientRecvDisruptor.handleEventsWith(new DeadlineBasedEventProcessor<>(_concentusHandle.getClock(), _clientProcessor, _clientRecvDisruptor.getRingBuffer(), recvBarrier, _concentusHandle));
		_clientSendDisruptor.handleEventsWith(_clientSocketReactor);
		_metricSendDisruptor.handleEventsWith(_metricPublisher);		
	}
	
	private void connect(ClusterWorkerHandle cluster) throws Exception {
		String metricListenerConnString = cluster.getServiceAtRandom(MetricListenerService.SERVICE_TYPE);
		int metricSubPort = _concentusHandle.getConfig().getServices().get(MetricListenerService.SERVICE_TYPE).getPorts().get("input");
		_socketManager.connectSocket(_metricPubSocketId, String.format("%s:%d", metricListenerConnString, metricSubPort));
		
		_metricSendDisruptor.start();
	}
	
	private void initTest(ClusterWorkerHandle cluster) throws Exception {
		// request client allocation
		byte[] reqBytes = new byte[4];
		MessageBytesUtil.writeInt(reqBytes, 0, _maxClients);
		cluster.requestAssignment(SERVICE_TYPE, reqBytes);
	}

	private void setUpTest(ClusterWorkerHandle cluster) throws Exception {
		// read in the number of clients to test with
		byte[] res = cluster.getAssignment(SERVICE_TYPE);
		if (res.length != 4) throw new RuntimeException("Expected an integer value");
		_clientCountForTest = MessageBytesUtil.readInt(res, 0);
		
		if (_clientCountForTest > _maxClients)
			throw new IllegalArgumentException(
					String.format("The client count was too large: %d > %d", 
							_clientCountForTest, 
							_maxClients));
	}
	
	private void startSUT(ClusterWorkerHandle cluster) throws Exception {
		String[] clientHandlerConnStrings = cluster.getAllServices(ClientHandlerService.SERVICE_TYPE);
		int clientHandlerPort = _concentusHandle.getConfig().getServices().get(ClientHandlerService.SERVICE_TYPE).getPorts().get("input");
		
		/*
		 * connect client socket to all client handlers
		 */
		_handlerIds = new int[clientHandlerConnStrings.length];
		for (int clientHandlerIndex = 0; clientHandlerIndex < clientHandlerConnStrings.length; clientHandlerIndex++) {
			String connString = String.format("%s:%d", 
					clientHandlerConnStrings[clientHandlerIndex],
					clientHandlerPort);
			int handlerSocketId = _socketManager.create(ZMQ.DEALER);
			_socketManager.connectSocket(handlerSocketId, connString);
			_handlerIds[clientHandlerIndex] = handlerSocketId;
		}
		
		/*
		 * assign client handler to each client and prepare clients
		 */
		int nextHandlerIndex = 0;
		for (long clientIndex = 0; clientIndex < _clients.getLength(); clientIndex++) {
			Client client = _clients.get(clientIndex);
			if (clientIndex < _clientCountForTest) {
				client.setHandlerId(_handlerIds[nextHandlerIndex++ % _handlerIds.length]);
				client.setIsActive(true);
			} else {
				client.setIsActive(false);
			}
			client.setIsConnecting(false);
		}
		
		SocketPollInSet clientHandlerSet = _socketManager.createPollInSet(_handlerIds);
		_clientSocketReactor.configure(clientHandlerSet, _clientSendHeader, _clientRecvHeader);
	}
	
	private void executeTest(ClusterWorkerHandle cluster) throws Exception {
		_clientRecvDisruptor.start();
		_clientSendDisruptor.start();
	}
	
	private void stopSendingInputEvents(ClusterWorkerHandle cluster) throws Exception {
		_clientProcessor.stopSendingInput();
	}
	
	private void teardown(ClusterWorkerHandle cluster) throws Exception {
		// must close first to stop incoming events
		_clientSendDisruptor.shutdown();		
		for (int handlerId : _handlerIds) {
			_socketManager.closeSocket(handlerId);
		}
		_handlerIds = null;
		
		// stop generating load
		_clientRecvDisruptor.shutdown();
		
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (client.isActive()) {
				client.setHandlerId(-1);
				client.setIsConnecting(false);
			}
		}
	}
	
	private void shutdown(ClusterWorkerHandle cluster) throws Exception {
		_metricSendDisruptor.shutdown();
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
		_socketManager.close();
	}
	
}
