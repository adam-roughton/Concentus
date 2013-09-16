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
package com.adamroughton.concentus.canonicalstate;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.cluster.data.ServiceEndpoint;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase;
import com.adamroughton.concentus.cluster.worker.StateData;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
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
import com.adamroughton.concentus.util.StatefulRunnable;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.zeromq.*;

import static com.adamroughton.concentus.Constants.MSG_BUFFER_ENTRY_LENGTH;

public class CanonicalStateService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase {
	
	public final static String SERVICE_TYPE = "canonical_state";

	private final ConcentusHandle _concentusHandle;
	private final MetricContext _metricContext;
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final SocketManager<TBuffer> _socketManager;
	private final EventQueue<TBuffer> _inputQueue;
	private final EventQueue<TBuffer> _outputQueue;
	private final OutgoingEventHeader _pubHeader;
	private final IncomingEventHeader _subHeader;
	private CollectiveApplication _application;
	
	private ProcessingPipeline<TBuffer> _pipeline;
	private StatefulRunnable<EventListener<TBuffer>> _subListener;
	private StateProcessor<TBuffer> _stateProcessor;
	private EventProcessor _publisher;	
	
	private final int _pubPort;
	private final int _pubSocketId;
	private final int _inputSocketId;
	
	public CanonicalStateService(
			int recvPort,
			int pubPort,
			int recvBufferLength,
			int pubBufferLength,
			ConcentusHandle concentusHandle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = resolver.newSocketManager(_concentusHandle.getClock());

		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager.newMessageQueueFactory(resolver.getEventQueueFactory());
		
		_inputQueue = messageQueueFactory.createSingleProducerQueue("inputQueue", 
				recvBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		
		_outputQueue = messageQueueFactory.createSingleProducerQueue("pubQueue", 
				pubBufferLength, 
				MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		
		_pubHeader = new OutgoingEventHeader(0, 2);
		_subHeader = new IncomingEventHeader(0, 1);
		
		/*
		 * Configure sockets
		 */
		// sub socket
		SocketSettings subSocketSettings = SocketSettings.create()
				.bindToPort(recvPort)
				.setHWM(1000)
				.subscribeToAll();
		_inputSocketId = _socketManager.create(ZMQ.SUB, subSocketSettings, "input");
		
		// pub socket
		SocketSettings pubSocketSettings = SocketSettings.create()
				.bindToPort(pubPort)
				.setHWM(1000);
		_pubSocketId = _socketManager.create(ZMQ.PUB, pubSocketSettings, "pub");
		int[] pubPorts = _socketManager.getBoundPorts(_pubSocketId);
		_pubPort = pubPorts[0];
	}
	
	@Override
	protected void onInit(StateData<ServiceState> stateData, ClusterHandle cluster) throws Exception {
		_application = cluster.getApplicationInstanceFactory().newInstance();
	}
	
	@Override
	protected void onBind(StateData<ServiceState> stateData, ClusterHandle cluster) throws Exception {
		// infrastructure for sub socket
		Mutex<Messenger<TBuffer>> subSocketPackageMutex = _socketManager.getSocketMutex(_inputSocketId);
		_subListener = Util.asStateful(new EventListener<>("inputListener", _subHeader, subSocketPackageMutex, _inputQueue, _concentusHandle));
		
		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader, TBuffer> pubSendQueue = new SendQueue<>("publisher", _pubHeader, _outputQueue);
		Mutex<Messenger<TBuffer>> pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		_publisher = MessagingUtil.asSocketOwner("publisher", _outputQueue, new Publisher<TBuffer>(_pubHeader), pubSocketPackageMutex);
		
		//_stateProcessor = new StateProcessor<>(_concentusHandle.getClock(), _stateLogic, _subHeader, pubSendQueue, _metricContext);
		
		_pipeline = ProcessingPipeline.<TBuffer>build(_subListener, _concentusHandle.getClock())
				.thenConnector(_inputQueue)
				.then(_inputQueue.createEventProcessor("stateProcessor", _stateProcessor, _metricContext,
						_concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_outputQueue)
				.then(_publisher)
				.createPipeline(_executor);
		
		ServiceEndpoint pubEndpoint = new ServiceEndpoint(ConcentusEndpoints.CANONICAL_STATE_PUB.getId(), 
				_concentusHandle.getNetworkAddress().getHostAddress(), _pubPort);
		cluster.registerServiceEndpoint(pubEndpoint);
	}

	@Override
	protected void onStart(StateData<ServiceState> stateData, ClusterHandle cluster) {
		_pipeline.start();
	}

	@Override
	protected void onShutdown(StateData<ServiceState> stateData, ClusterHandle cluster) throws Exception {
		cluster.unregisterServiceEndpoint(ConcentusEndpoints.CANONICAL_STATE_PUB.getId());
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}
	
}
