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
import com.adamroughton.consentus.canonicalstate.CanonicalStateService;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.crowdhammer.CrowdHammerService;
import com.adamroughton.consentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.SocketPackage;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class MetricListenerService implements CrowdHammerService {
	
	public static final String SERVICE_TYPE = "MetricListener";
	
	private static final Logger LOG = Logger.getLogger(MetricListenerService.class.getName());
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _inputDisruptor;
	
	private EventListener _eventListener;
	private Future<?> _eventListenerTask;
	private MetricProcessor _metricProcessor;
	
	private ZMQ.Context _zmqContext;
	private ZMQ.Socket _subSocket;
	
	private CrowdHammerConfiguration _config;
	private InetAddress _networkAddress;
	private ConsentusProcessCallback _exHandler;
	
	private SocketSettings _subSocketSettings;

	@Override
	public String name() {
		return "Metric Listener Service";
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			Cluster cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		if (newClusterState == CrowdHammerServiceState.SET_UP_TEST) {
			setUpTest(cluster);
		} else if (newClusterState == CrowdHammerServiceState.CONNECT_SUT) {
			connectSUT(cluster);
		} else if (newClusterState == CrowdHammerServiceState.TEAR_DOWN) {
			tearDown(cluster);
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
	@Override
	public void configure(CrowdHammerConfiguration config,
			ConsentusProcessCallback exHandler, InetAddress networkAddress) {
		_config = config;
		_exHandler = exHandler;
		_networkAddress = networkAddress;
		
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		
		_inputDisruptor = new Disruptor<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
			
		}, _executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		_inputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Input Disruptor", exHandler));
		
		_metricProcessor = new MetricProcessor();
		_inputDisruptor.handleEventsWith(_metricProcessor);
		
		int testMetricsSubPort = config.getServices().get(SERVICE_TYPE).getPorts().get("input");
		_subSocketSettings = SocketSettings.create()
				.bindToPort(testMetricsSubPort)
				.subscribeTo(EventType.STATE_METRIC);	
	}
	
	private void setUpTest(Cluster cluster) throws Exception {
		_subSocket = _zmqContext.socket(ZMQ.SUB);
		_subSocketSettings.configureSocket(_subSocket);
		SocketPackage socketPackage = SocketPackage.create(_subSocket) 
				.setMessageOffsets(0, 0);
		_eventListener = new EventListener(socketPackage, _inputDisruptor.getRingBuffer(), _zmqContext, _exHandler);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _networkAddress.getHostAddress()));
	}
	
	private void connectSUT(Cluster cluster) {
		
		int metricsPort = _config.getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("pub");
		for (String service : cluster.getAllServices(CanonicalStateService.SERVICE_TYPE)) {
			_subSocket.connect(String.format("%s:%d", service, metricsPort));
		}
		_eventListenerTask = _executor.submit(_eventListener);
		_inputDisruptor.start();
	}
	
	private void tearDown(Cluster cluster) {
		_eventListenerTask.cancel(true);
		
		// persist results to file
		
		_inputDisruptor.shutdown();
		_subSocket.close();		
	}
	
	
	private void shutdown(Cluster cluster) {
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
		_zmqContext.term();
	}
}
