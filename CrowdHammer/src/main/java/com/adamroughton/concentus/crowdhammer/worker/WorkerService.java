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
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.SendRecvMessengerReactor;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.lmax.disruptor.YieldingWaitStrategy;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.concentus.util.Util.*;

public final class WorkerService<TBuffer extends ResizingBuffer> implements CrowdHammerService {

	public static final String SERVICE_TYPE = "CrowdHammerWorker";
	private static final Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final ConcentusHandle<? extends CrowdHammerConfiguration, TBuffer> _concentusHandle;
	private final MetricContext _metricContext;
	
	private EventQueue<TBuffer> _clientRecvQueue;
	private EventQueue<TBuffer> _clientSendQueue;
	
	private ProcessingPipeline<TBuffer> _pipeline;
	
	private final OutgoingEventHeader _clientSendHeader;
	private final IncomingEventHeader _clientRecvHeader;
	
	private SocketManager<TBuffer> _socketManager;
	private final int _maxClients;
	private final StructuredArray<Client> _clients;
	
	private int _clientCountForTest;
	private SimulatedClientProcessor<TBuffer> _clientProcessor;
	
	private SendRecvMessengerReactor<TBuffer> _clientSocketReactor;
	private int[] _handlerIds;
	
	public WorkerService(final ConcentusHandle<? extends CrowdHammerConfiguration, TBuffer> concentusHandle, int maxClientCount, MetricContext metricContext) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_maxClients = maxClientCount;
		_metricContext = Objects.requireNonNull(metricContext);
		_clients = StructuredArray.newInstance(nextPowerOf2(_maxClients), Client.class, new ComponentFactory<Client>() {

			long index = 0;
			
			@Override
			public Client newInstance(Object[] initArgs) {
				return new Client(index++, _concentusHandle.getClock());
			}
		});
		_clientSendHeader = new OutgoingEventHeader(0, 1);
		_clientRecvHeader = new IncomingEventHeader(0, 1);
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			ClusterWorkerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		if (newClusterState == CrowdHammerServiceState.INIT_TEST) {
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
	
	private void initTest(ClusterWorkerHandle cluster) throws Exception {
		_socketManager = _concentusHandle.newSocketManager();
		
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
		
		CrowdHammerConfiguration config = _concentusHandle.getConfig();
		int routerRecvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerRecv");
		int routerSendBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "routerSend");
		
		MessageQueueFactory<TBuffer> messageQueueFactory = 
				_socketManager.newMessageQueueFactory(_concentusHandle.getEventQueueFactory());
		
		_clientRecvQueue = messageQueueFactory.createSingleProducerQueue(
				"clientRecvQueue", 
				routerRecvBufferLength,
				Constants.DEFAULT_MSG_BUFFER_SIZE,
				new YieldingWaitStrategy());
		_clientSendQueue = messageQueueFactory.createSingleProducerQueue(
				"clientSendQueue",
				routerSendBufferLength, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, 
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
			SocketSettings socketSettings = SocketSettings.create().setSupportReliable(true);
			int handlerSocketId = _socketManager.create(ZMQ.DEALER, socketSettings, "clientHandler" + clientHandlerIndex + " (" + connString + ")");
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
		
		Mutex<Messenger<TBuffer>> clientHandlerSet = _socketManager.createPollInSet(_handlerIds);
		
		// infrastructure for client socket
		_clientSocketReactor = new SendRecvMessengerReactor<>(
				"clientSocketReactor",
				clientHandlerSet, 
				_clientSendHeader, 
				_clientRecvHeader,
				_clientRecvQueue,
				_clientSendQueue, 
				_concentusHandle);

		// event processing infrastructure
		SendQueue<OutgoingEventHeader, TBuffer> clientSendQueue = new SendQueue<>("clientProcessor", _clientSendHeader, _clientSendQueue);
		_clientProcessor = new SimulatedClientProcessor<>(_concentusHandle.getClock(), _clients, _clientCountForTest, clientSendQueue, _clientRecvHeader, _metricContext);
		
		_pipeline = ProcessingPipeline.<TBuffer>startCyclicPipeline(_clientSendQueue, _concentusHandle.getClock())
				.then(_clientSocketReactor)
				.thenConnector(_clientRecvQueue)
				.then(_clientRecvQueue.createEventProcessor("clientProcessor", _clientProcessor, _concentusHandle.getClock(), _concentusHandle))
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
