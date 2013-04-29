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
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.clienthandler.ClientHandlerService;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.crowdhammer.CrowdHammerService;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.crowdhammer.messaging.events.TestEventType;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.SocketManager;
import com.adamroughton.concentus.messaging.SocketMutex;
import com.adamroughton.concentus.messaging.SocketSettings;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.zeromq.*;

public class MetricListenerService implements CrowdHammerService {
	
	public static final String SERVICE_TYPE = "MetricListener";
	
	private static final Logger LOG = Logger.getLogger(MetricListenerService.class.getName());
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final ConcentusHandle<? extends CrowdHammerConfiguration> _concentusHandle;
	private SocketManager _socketManager;
	private RingBuffer<byte[]> _inputBuffer;
	private final IncomingEventHeader _header;
	
	private ProcessingPipeline<byte[]> _pipeline;
	
	private MetricEventProcessor _metricProcessor;
	private EventListener _eventListener;
	
	private final SocketSettings _subSocketSettings;
	private int _subSocketId;
	private final IntSet _sutMetricConnIdSet = new IntArraySet();
	
	private int _clientCount = 0;
	
	public MetricListenerService(ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_header = new IncomingEventHeader(0, 2);
		
		int testMetricsSubPort = _concentusHandle.getConfig().getServices().get(SERVICE_TYPE).getPorts().get("input");
		_subSocketSettings = SocketSettings.create()
				.bindToPort(testMetricsSubPort)
				.subscribeTo(EventType.STATE_METRIC)
				.subscribeTo(TestEventType.WORKER_METRIC.getId())
				.subscribeTo(EventType.CLIENT_HANDLER_METRIC.getId());
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			ClusterWorkerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		if (newClusterState == CrowdHammerServiceState.INIT_TEST) {
			onInitTest(cluster);
		} else if (newClusterState == CrowdHammerServiceState.SET_UP_TEST) { 
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
	
	private void onInitTest(ClusterWorkerHandle cluster) {
		_socketManager = new SocketManager();
		_subSocketId = _socketManager.create(ZMQ.SUB, _subSocketSettings);
		
		// request sim client count
		cluster.requestAssignment(SERVICE_TYPE, new byte[0]);
	}
	
	private void onSetUpTest(ClusterWorkerHandle cluster) {		
		_socketManager.bindBoundSockets();
		
		_inputBuffer = new RingBuffer<>(Util.msgBufferFactory(Constants.MSG_BUFFER_LENGTH), 
				new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		
		SocketMutex socketPackage = _socketManager.getSocketMutex(_subSocketId);
		_eventListener = new EventListener(_header, socketPackage, _inputBuffer, _concentusHandle);
		
		_metricProcessor = new MetricEventProcessor(_header);
		
		_pipeline = ProcessingPipeline.<byte[]>build(_eventListener, _concentusHandle.getClock())
				.thenConnector(_inputBuffer)
				.then(new BatchEventProcessor<>(_inputBuffer, _inputBuffer.newBarrier(), _metricProcessor))
				.createPipeline(_executor);
						
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _concentusHandle.getNetworkAddress().getHostAddress()));
		
		// get client count
		int cursor = 0;
		byte[] assignment = cluster.getAssignment(SERVICE_TYPE);
		if (assignment.length < 4) 
			throw new RuntimeException(String.format("Expected the assignment to be at least one integer (4 bytes) long, " +
					"instead had length %d", assignment.length));
		int workerCount = MessageBytesUtil.readInt(assignment, 0);
		WorkerInfo[] workers = new WorkerInfo[workerCount];
		cursor += 4;
		for (int i = 0; i < workerCount; i++) {
			long workerId = MessageBytesUtil.readLong(assignment, cursor);
			cursor += 8;
			int clientCount = MessageBytesUtil.readInt(assignment, cursor);
			cursor += 4;
			workers[i] = new WorkerInfo(workerId, clientCount);
		}
		_metricProcessor.setActiveWorkers(workers);
	}
	
	private void onConnectSUT(ClusterWorkerHandle cluster) {
		int canoncicalPubPort = _concentusHandle.getConfig().getServices().get(CanonicalStateService.SERVICE_TYPE).getPorts().get("pub");
		for (String service : cluster.getAllServices(CanonicalStateService.SERVICE_TYPE)) {
			_sutMetricConnIdSet.add(_socketManager.connectSocket(_subSocketId, String.format("%s:%d", service, canoncicalPubPort)));
		}
		int clientHandlerPubPort = _concentusHandle.getConfig().getServices().get(ClientHandlerService.SERVICE_TYPE).getPorts().get("pub");
		for (String service : cluster.getAllServices(ClientHandlerService.SERVICE_TYPE)) {
			_sutMetricConnIdSet.add(_socketManager.connectSocket(_subSocketId, String.format("%s:%d", service, clientHandlerPubPort)));
		}
		
		_pipeline.start();
	}
	
	private void onTearDown(ClusterWorkerHandle cluster) throws Exception {
		_pipeline.halt(60, TimeUnit.SECONDS);
				
		// persist results to file
		_metricProcessor.endOfTest();
		
		// close the metric socket
		_socketManager.close();
		_sutMetricConnIdSet.clear();
	}
	
	
	private void onShutdown(ClusterWorkerHandle cluster) throws Exception {
		_metricProcessor.closeOpenFiles();
	}
}
