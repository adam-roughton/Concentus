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
package com.adamroughton.consentus.crowdhammer.worker;

import java.net.InetAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.Constants;
import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.clienthandler.ClientHandlerService;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.crowdhammer.CrowdHammerService;
import com.adamroughton.consentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.consentus.Util.*;

public final class WorkerService implements CrowdHammerService {

	public static final String SERVICE_TYPE = "CrowdHammerWorker";
	
	private final ZMQ.Context _zmqContext;
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final Disruptor<byte[]> _metricSendQueue;
	
	// we allocate to the next power of 2 to make the wrapping around operation faster
	private int _maxClients;
	private int _clientCountForTest;
	private StructuredArray<Client> _clients;
	private ClientReactor _clientReactor;
	
	private CrowdHammerConfiguration _config;
	private FatalExceptionCallback _exHandler;
	private InetAddress _networkAddress;
	
	private Future<?> _runningTest = null;
	
	public WorkerService() {
		_metricSendQueue = new Disruptor<>(
				Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), 
				_executor, 
				new SingleThreadedClaimStrategy(2048), 
				new YieldingWaitStrategy());
		
		_zmqContext = ZMQ.context(1);
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			Cluster cluster) throws Exception {
		if (newClusterState == CrowdHammerServiceState.INIT) {
			init(cluster);
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
		
		cluster.signalReady();
	}

	@Override
	public Class<CrowdHammerServiceState> getStateValueClass() {
		return CrowdHammerServiceState.class;
	}
	
	@Override
	public void configure(CrowdHammerConfiguration config,
			ConsentusProcessCallback exHandler, 
			InetAddress networkAddress) {
		_config = config;
		_exHandler = Objects.requireNonNull(exHandler);
		_networkAddress = networkAddress;
	}
	
	public void setMaxClientCount(final int maxClientCount) {
		_maxClients = maxClientCount;
	}
	
	private void init(Cluster cluster) throws Exception {
		_clients = StructuredArray.newInstance(nextPowerOf2(_maxClients), Client.class, new Class[] {ZMQ.Context.class}, _zmqContext);
		_clientReactor = new ClientReactor(_clients, _metricSendQueue.getRingBuffer(), _metricSendQueue.getRingBuffer().newBarrier());
	}
	
	private void initTest(Cluster cluster) throws Exception {
		// request client allocation
		byte[] reqBytes = new byte[4];
		MessageBytesUtil.writeInt(reqBytes, 0, _maxClients);
		cluster.requestAssignment(SERVICE_TYPE, reqBytes);
	}

	private void setUpTest(Cluster cluster) throws Exception {
		// read in the number of clients to test with
		byte[] res = cluster.getAssignment(SERVICE_TYPE);
		if (res.length != 4) throw new RuntimeException("Expected an integer value");
		int _clientCountForTest = MessageBytesUtil.readInt(res, 0);
		
		if (_clientCountForTest > _maxClients)
			throw new IllegalArgumentException(
					String.format("The client count was too large: %d > %d", 
							_clientCountForTest, 
							_maxClients));
	}
	
	private void startSUT(Cluster cluster) throws Exception {
		String[] clientHandlerConnStrings = cluster.getAllServices(ClientHandlerService.SERVICE_TYPE);
		int clientHandlerPort = _config.getServices().get(ClientHandlerService.SERVICE_TYPE).getPorts().get("input");
		
		int nextConnString = 0;
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (i < _clientCountForTest) {
				ZMQ.Socket clientSocket = client.getSocket();
				String connString = String.format("%s:%d", 
						clientHandlerConnStrings[nextConnString++ % clientHandlerConnStrings.length],
						clientHandlerPort);
				client.setClientHandlerConnString(connString);
				clientSocket.connect(connString);
				client.setIsActive(true);
				try {
					Thread.sleep(10);
				} catch (InterruptedException eInterrupt) {
				}
			} else {
				client.setIsActive(false);
			}
		}
	}
	
	private void executeTest(Cluster cluster) throws Exception {
		_runningTest = _executor.submit(_clientReactor);
	}
	
	private void stopSendingInputEvents(Cluster cluster) throws Exception {
		_clientReactor.stopSendingInput();
	}
	
	private void teardown(Cluster cluster) throws Exception {
		_clientReactor.halt();
		Client client;
		for (int i = 0; i < _clients.getLength(); i++) {
			client = _clients.get(i);
			if (client.isActive()) {
				String connString = _clients.get(i).getClientHandlerConnString();
				_clients.get(i).getSocket().disconnect(connString);
			}
		}
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

	@Override
	public String name() {
		return "CrowdHammer Worker";
	}
	
}
