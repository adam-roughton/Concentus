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
package com.adamroughton.consentus.crowdhammer.metriclistener;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.Constants;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.canonicalstate.CanonicalStateService;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.crowdhammer.CrowdHammerService;
import com.adamroughton.consentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.SocketManager;
import com.adamroughton.consentus.messaging.SocketPackage;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.events.EventType;
import com.adamroughton.consentus.messaging.patterns.SubRecvQueueReader;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class MetricListenerService implements CrowdHammerService {
	
	public static final String SERVICE_TYPE = "MetricListener";
	
	private static final Logger LOG = Logger.getLogger(MetricListenerService.class.getName());
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final SocketManager _socketManager;
	private final Disruptor<byte[]> _inputDisruptor;
	private final EventProcessingHeader _header;
	
	private MetricProcessor _metricProcessor;
	private EventListener _eventListener;
	private Future<?> _eventListenerTask;
	
	private int _subSocketId;
	
	private CrowdHammerConfiguration _config;
	private InetAddress _networkAddress;
	private ConsentusProcessCallback _exHandler;
	
	public MetricListenerService() {
		_socketManager = new SocketManager();
		
		_inputDisruptor = new Disruptor<>(Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), _executor, 
				new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		
		_header = new EventProcessingHeader(0, 1);
	}
	
	@Override
	public String name() {
		return "Metric Listener Service";
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			Cluster cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		if (newClusterState == CrowdHammerServiceState.SET_UP_TEST) {
			onSetUpTest(cluster);
		} else if (newClusterState == CrowdHammerServiceState.CONNECT_SUT) {
			onConnectSUT(cluster);
		} else if (newClusterState == CrowdHammerServiceState.TEAR_DOWN) {
			onTearDown(cluster);
		} else if (newClusterState == CrowdHammerServiceState.SHUTDOWN) {
			onShutdown(cluster);
		}
		LOG.info("Signalling ready for next state");
		cluster.signalReady();
	}

	@Override
	public Class<CrowdHammerServiceState> getStateValueClass() {
		return CrowdHammerServiceState.class;
	}

	@Override
	public void configure(CrowdHammerConfiguration config,
			ConsentusProcessCallback exHandler, InetAddress networkAddress) {
		_config = config;
		_exHandler = exHandler;
		_networkAddress = networkAddress;
		
		_inputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Input Disruptor", exHandler));
		
		int testMetricsSubPort = config.getServices().get(SERVICE_TYPE).getPorts().get("input");
		SocketSettings subSocketSettings = SocketSettings.create()
				.bindToPort(testMetricsSubPort)
				.subscribeTo(EventType.STATE_METRIC);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSettings);
	}
	
	@SuppressWarnings("unchecked")
	private void onSetUpTest(Cluster cluster) throws Exception {
		_socketManager.bindBoundSockets();
		
		SubRecvQueueReader subRecvQueueReader = new SubRecvQueueReader(_header);
		SocketPackage socketPackage = _socketManager.createSocketPackage(_subSocketId, subRecvQueueReader.getMessageFrameBufferMapping());
		_eventListener = new EventListener(socketPackage, _inputDisruptor.getRingBuffer(), _exHandler);
		
		_metricProcessor = new MetricProcessor(subRecvQueueReader);
		_inputDisruptor.handleEventsWith(_metricProcessor);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _networkAddress.getHostAddress()));
	}
	
	private void onConnectSUT(Cluster cluster) {
		int metricsPort = _config.getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("pub");
		for (String service : cluster.getAllServices(CanonicalStateService.SERVICE_TYPE)) {
			_socketManager.connectSocket(_subSocketId, String.format("%s:%d", service, metricsPort));
		}
		_eventListenerTask = _executor.submit(_eventListener);
		_inputDisruptor.start();
	}
	
	private void onTearDown(Cluster cluster) {
		_eventListenerTask.cancel(true);
		
		// persist results to file
		
		_inputDisruptor.shutdown();
		_socketManager.closeManagedSockets();		
	}
	
	
	private void onShutdown(Cluster cluster) {
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
		_socketManager.close();
	}
}
