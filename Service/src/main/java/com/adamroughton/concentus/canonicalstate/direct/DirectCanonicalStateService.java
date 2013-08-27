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
package com.adamroughton.concentus.canonicalstate.direct;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusService;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.actioncollector.ActionCollectorService;
import com.adamroughton.concentus.canonicalstate.TickTimer;
import com.adamroughton.concentus.cluster.worker.ClusterListenerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.zeromq.*;

import static com.adamroughton.concentus.Constants.MSG_BUFFER_ENTRY_LENGTH;

public class DirectCanonicalStateService<TBuffer extends ResizingBuffer> implements ConcentusService {
	
	public final static String SERVICE_TYPE = "CanonicalState";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);

	private final ConcentusHandle<? extends Configuration, TBuffer> _concentusHandle;
	private final MetricContext _metricContext;
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final SocketManager<TBuffer> _socketManager;
	private final EventQueue<TBuffer> _outputQueue;
	private final OutgoingEventHeader _pubHeader;
	private final EventQueue<ComputeStateEvent> _recvQueue;
	private CollectiveApplication _application;
	
	private ProcessingPipeline<ComputeStateEvent> _recvPipeline;
	private ProcessingPipeline<TBuffer> _pubPipeline;
	private DirectStateProcessor<TBuffer> _stateProcessor;
	private EventProcessor _publisher;
	private ActionCollectorService<TBuffer> _actionCollectorService;
	
	
	private final int _pubPort;
	private final int _pubSocketId;
	
	public DirectCanonicalStateService(ConcentusHandle<? extends Configuration, TBuffer> concentusHandle, MetricContext metricContext) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = _concentusHandle.newSocketManager();

		Configuration config = concentusHandle.getConfig();
		
		int pubBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "pub");
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager.newMessageQueueFactory(_concentusHandle.getEventQueueFactory());
		_outputQueue = messageQueueFactory.createSingleProducerQueue("pubQueue", 
				pubBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		
		_recvQueue = _concentusHandle.getEventQueueFactory().createSingleProducerQueue("recvQueue", 
				new EventFactory<ComputeStateEvent>() {

					@Override
					public ComputeStateEvent newInstance() {
						return new ComputeStateEvent();
					}}, 
				4, 
				new YieldingWaitStrategy());
		
		_pubHeader = new OutgoingEventHeader(0, 2);
		
		/*
		 * Configure pub socket
		 */
		_pubPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "pub");
		SocketSettings pubSocketSettings = SocketSettings.create()
				.bindToPort(_pubPort)
				.setHWM(1000);
		_pubSocketId = _socketManager.create(ZMQ.PUB, pubSocketSettings, "pub");
	}

	@Override
	public void onStateChanged(ConcentusServiceState newClusterState,
			ClusterListenerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		switch (newClusterState) {
			case INIT:
				onInit(cluster);
				break;
			case BIND:
				onBind(cluster);
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
	
	private void onInit(ClusterListenerHandle cluster) throws Exception {
		_application = Util.newInstance(cluster.getApplicationClass(), CollectiveApplication.class);
	}
	
	private void onBind(ClusterListenerHandle cluster) throws Exception {
		// infrastructure for pub socket
		
		Mutex<Messenger<TBuffer>> pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		_publisher = MessagingUtil.asSocketOwner("publisher", _outputQueue, new Publisher<TBuffer>(_pubHeader), pubSocketPackageMutex);
		
		cluster.registerServiceEndpoint(ConcentusEndpoints.CANONICAL_STATE_PUB, 
				_concentusHandle.getNetworkAddress().getHostAddress(),
				_pubPort);
		
		_actionCollectorService = new ActionCollectorService<TBuffer>(_concentusHandle, 
				new DirectTickDelegate<>(_recvQueue), 0, 0, _application.getTickDuration());
	}

	private void onStart(ClusterListenerHandle cluster) {
		SendQueue<OutgoingEventHeader, TBuffer> pubSendQueue = new SendQueue<>("publisher", _pubHeader, _outputQueue);
		
		Clock clock = _concentusHandle.getClock();
		_stateProcessor = new DirectStateProcessor<>(_application, clock, pubSendQueue, new TickTimer.TickStrategy() {
			
			@Override
			public void onTick(long time) {
				_actionCollectorService.tick(time);
			}
		}, _metricContext);		
		
		Runnable startPoint = new Runnable() {
			
			@Override
			public void run() {
			}
		};
		_recvPipeline = ProcessingPipeline.<ComputeStateEvent>build(startPoint, clock)
				.thenConnector(_recvQueue)
				.then(_recvQueue.createEventProcessor("Direct Canonical State Processor", _stateProcessor, clock, _concentusHandle))
				.createPipeline(_executor);
		_pubPipeline = ProcessingPipeline.<TBuffer>build(startPoint, clock)
				.thenConnector(_outputQueue)
				.then(_publisher)
				.createPipeline(_executor);
		
		_pubPipeline.start();
		_recvPipeline.start();
	}

	private void onShutdown(ClusterListenerHandle cluster) throws Exception {
		_recvPipeline.halt(60, TimeUnit.SECONDS);
		_pubPipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}

	@Override
	public Class<ConcentusServiceState> getStateValueClass() {
		return ConcentusServiceState.class;
	}
	
}
