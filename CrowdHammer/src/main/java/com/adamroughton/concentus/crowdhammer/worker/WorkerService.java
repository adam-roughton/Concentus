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

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.clienthandler.ClientHandlerService;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.crowdhammer.CrowdHammerService;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.SingleProducerEventQueue;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SendRecvMessengerReactor;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.pipeline.PipelineBranch;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.concentus.util.Util.*;

public final class WorkerService implements CrowdHammerService {

	public static final String SERVICE_TYPE = "CrowdHammerWorker";
	private static final Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final ConcentusHandle<? extends CrowdHammerConfiguration> _concentusHandle;
	
	private EventQueue<byte[]> _clientRecvQueue;
	private EventQueue<byte[]> _clientSendQueue;
	private EventQueue<byte[]> _metricSendQueue;
	
	private ProcessingPipeline<byte[]> _pipeline;
	
	private final OutgoingEventHeader _clientSendHeader;
	private final IncomingEventHeader _clientRecvHeader;
	private final OutgoingEventHeader _metricSendHeader;
	
	private SocketManager _socketManager;
	private int _metricPubSocketId;
	private final int _maxClients;
	private final StructuredArray<Client> _clients;
	
	private long _workerId = -1;
	private int _clientCountForTest;
	private SimulatedClientProcessor _clientProcessor;
	private EventProcessor _metricPublisher;
	
	private SendRecvMessengerReactor _clientSocketReactor;
	private int[] _handlerIds;
	
	public WorkerService(final ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle, int maxClientCount) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_maxClients = maxClientCount;
		_clients = StructuredArray.newInstance(nextPowerOf2(_maxClients), Client.class, new ComponentFactory<Client>() {

			long index = 0;
			
			@Override
			public Client newInstance(Object[] initArgs) {
				return new Client(index++, _concentusHandle.getClock());
			}
		});
		_clientSendHeader = new OutgoingEventHeader(0, 1);
		_clientRecvHeader = new IncomingEventHeader(0, 1);
		_metricSendHeader = new OutgoingEventHeader(0, 2);
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
	
	private void init(ClusterWorkerHandle cluster) throws Exception {		
		// request workerId
		cluster.requestAssignment(SERVICE_TYPE, new byte[0]);
	}
	
	private void connect(ClusterWorkerHandle cluster) throws Exception {
		// read in the workerId
		byte[] res = cluster.getAssignment(SERVICE_TYPE);
		if (res.length != 8) throw new RuntimeException("Expected a long value");
		_workerId = MessageBytesUtil.readLong(res, 0);
	}
	
	private void initTest(ClusterWorkerHandle cluster) throws Exception {
		_socketManager = _concentusHandle.newSocketManager();
		
		// request client allocation
		byte[] reqBytes = new byte[4];
		MessageBytesUtil.writeInt(reqBytes, 0, _maxClients);
		cluster.requestAssignment(SERVICE_TYPE, reqBytes);

		// metric socket
		int metricsPubPort = ConfigurationUtil.getPort(_concentusHandle.getConfig(), SERVICE_TYPE, "pub");
		SocketSettings metricSocketSettings = SocketSettings.create()
				.bindToPort(metricsPubPort);
		_metricPubSocketId = _socketManager.create(ZMQ.PUB, metricSocketSettings);
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
		
		CrowdHammerConfiguration config = _concentusHandle.getConfig();
		int routerRecvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerRecv");
		int routerSendBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerSend");
		int metricBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "metric");
		
		_clientRecvQueue = new SingleProducerEventQueue<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_ENTRY_LENGTH), 
				routerRecvBufferLength, 
				new YieldingWaitStrategy());
		_clientSendQueue = new SingleProducerEventQueue<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_ENTRY_LENGTH), 
				routerSendBufferLength, 
				new YieldingWaitStrategy());
		_metricSendQueue = new SingleProducerEventQueue<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_ENTRY_LENGTH), 
				metricBufferLength, 
				new YieldingWaitStrategy());
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _concentusHandle.getNetworkAddress().getHostAddress()));
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
		}
		
		Mutex<Messenger> clientHandlerSet = _socketManager.createPollInSet(_handlerIds);
		
		// infrastructure for client socket
		SendQueue<OutgoingEventHeader> clientSendQueue = new SendQueue<>(_clientSendHeader, _clientSendQueue);
		_clientSocketReactor = new SendRecvMessengerReactor(
				clientHandlerSet, 
				_clientSendHeader, 
				_clientRecvHeader,
				_clientRecvQueue,
				_clientSendQueue, 
				_concentusHandle);

		// infrastructure for metric socket
		SendQueue<OutgoingEventHeader> metricSendQueue = new SendQueue<>(_metricSendHeader, _metricSendQueue);
		Mutex<Messenger> pubSocketPackageMutex = _socketManager.getSocketMutex(_metricPubSocketId);
		_metricPublisher = MessagingUtil.asSocketOwner(_metricSendQueue, new Publisher(_metricSendHeader), pubSocketPackageMutex);
		
		// event processing infrastructure
		_clientProcessor = new SimulatedClientProcessor(_workerId, _concentusHandle.getClock(), _clients, _clientCountForTest, clientSendQueue, metricSendQueue, _clientRecvHeader);
		
		PipelineBranch<byte[]> metricSendBranch = ProcessingPipeline.startBranch(_metricSendQueue, _concentusHandle.getClock())
				.then(_metricPublisher)
				.create();
		
		_pipeline = ProcessingPipeline.<byte[]>startCyclicPipeline(_clientSendQueue, _concentusHandle.getClock())
				.then(_clientSocketReactor)
				.thenConnector(_clientRecvQueue)
				.then(_clientRecvQueue.createDeadlineBasedEventProcessor(_clientProcessor, _concentusHandle.getClock(), _concentusHandle))
				.attachBranch(metricSendBranch)
				.completeCycle(_executor);
	}
	
	private void executeTest(ClusterWorkerHandle cluster) throws Exception {
		_pipeline.start();
	}
	
	private void stopSendingInputEvents(ClusterWorkerHandle cluster) throws Exception {
		_clientProcessor.stopSendingInput();
	}
	
	private void teardown(ClusterWorkerHandle cluster) throws Exception {		
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
		_handlerIds = null;
		
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (client.isActive()) {
				client.reset();
			}
		}
	}
	
	private void shutdown(ClusterWorkerHandle cluster) throws Exception {
	}
	
}
