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
package com.adamroughton.concentus.crowdhammer.metriclistener;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.crowdhammer.CrowdHammerService;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.SocketManager;
import com.adamroughton.concentus.messaging.SocketPackage;
import com.adamroughton.concentus.messaging.SocketSettings;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.util.StatefulRunnable;
import com.adamroughton.concentus.util.Util;
import com.adamroughton.concentus.util.StatefulRunnable.State;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class MetricListenerService implements CrowdHammerService {
	
	public static final String SERVICE_TYPE = "MetricListener";
	
	private static final Logger LOG = Logger.getLogger(MetricListenerService.class.getName());
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final ConcentusHandle<? extends CrowdHammerConfiguration> _concentusHandle;
	private final SocketManager _socketManager;
	private final Disruptor<byte[]> _inputDisruptor;
	private final IncomingEventHeader _header;
	
	private MetricProcessor _metricProcessor;
	private StatefulRunnable<EventListener> _eventListener;
	private Future<?> _eventListenerTask;
	
	private final int _subSocketId;
	private final IntSet _sutMetricConnIdSet = new IntArraySet();
	
	public MetricListenerService(ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_socketManager = new SocketManager();
		_inputDisruptor = new Disruptor<>(Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), _executor, 
				new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		_header = new IncomingEventHeader(0, 2);
		
		_inputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Input Disruptor", _concentusHandle));
		
		int testMetricsSubPort = _concentusHandle.getConfig().getServices().get(SERVICE_TYPE).getPorts().get("input");
		SocketSettings subSocketSettings = SocketSettings.create()
				.bindToPort(testMetricsSubPort)
				.subscribeTo(EventType.STATE_METRIC);
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSettings);
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			ClusterWorkerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		if (newClusterState == CrowdHammerServiceState.BIND) {
			onBind(cluster);
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
	
	@SuppressWarnings("unchecked")
	private void onBind(ClusterWorkerHandle cluster) throws Exception {
		_socketManager.bindBoundSockets();
		
		SocketPackage socketPackage = _socketManager.createSocketPackage(_subSocketId);
		_eventListener = Util.asStateful(new EventListener(_header, socketPackage, _inputDisruptor.getRingBuffer(), _concentusHandle));
		_socketManager.addDependency(_subSocketId, _eventListener);
		
		_metricProcessor = new MetricProcessor(_header);
		_inputDisruptor.handleEventsWith(_metricProcessor);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _concentusHandle.getNetworkAddress().getHostAddress()));
	}
	
	private void onConnectSUT(ClusterWorkerHandle cluster) {
		int metricsPort = _concentusHandle.getConfig().getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("pub");
		for (String service : cluster.getAllServices(CanonicalStateService.SERVICE_TYPE)) {
			_sutMetricConnIdSet.add(_socketManager.connectSocket(_subSocketId, String.format("%s:%d", service, metricsPort)));
		}
		_eventListenerTask = _executor.submit(_eventListener);
		_inputDisruptor.start();
	}
	
	private void onTearDown(ClusterWorkerHandle cluster) {
		_eventListenerTask.cancel(true);
		try {
			_eventListener.waitForState(State.STOPPED, 30, TimeUnit.SECONDS);
			if (_eventListener.getState() != State.STOPPED) {
				throw new RuntimeException("The event listener did not stop within the timeout.");
			}
		} catch (InterruptedException eInterrupted) {
			throw new RuntimeException("Interrupted while stopping the event listener - listener is now in an undefined state");
		}
		
		// persist results to file
		
		_inputDisruptor.shutdown();
		
		// disconnect from the SUT
		for (int connId : _sutMetricConnIdSet) {
			_socketManager.disconnectSocket(_subSocketId, connId);
		}
		_sutMetricConnIdSet.clear();
	}
	
	
	private void onShutdown(ClusterWorkerHandle cluster) {
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
		_socketManager.close();
	}
}
