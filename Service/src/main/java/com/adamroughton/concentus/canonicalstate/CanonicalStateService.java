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

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusService;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.StatefulRunnable;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.zeromq.*;

import static com.adamroughton.concentus.Constants.MSG_BUFFER_ENTRY_LENGTH;
import static com.adamroughton.concentus.util.Util.msgBufferFactory;

public class CanonicalStateService implements ConcentusService {
	
	public final static String SERVICE_TYPE = "CanonicalState";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);

	private final ConcentusHandle<? extends Configuration> _concentusHandle;
	private final MetricContext _metricContext;
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final SocketManager _socketManager;
	private final EventQueue<byte[]> _inputQueue;
	private final EventQueue<byte[]> _outputQueue;
	private final OutgoingEventHeader _pubHeader;
	private final IncomingEventHeader _subHeader;
	private final StateLogic _stateLogic;
	
	private ProcessingPipeline<byte[]> _pipeline;
	private StatefulRunnable<EventListener> _subListener;
	private StateProcessor _stateProcessor;
	private EventProcessor _publisher;	
	
	private final int _pubSocketId;
	private final int _inputSocketId;
	
	public CanonicalStateService(ConcentusHandle<? extends Configuration> concentusHandle, MetricContext metricContext) {
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricContext = Objects.requireNonNull(metricContext);
		_socketManager = _concentusHandle.newSocketManager();

		Configuration config = concentusHandle.getConfig();
		
		int recvBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "recv");
		int pubBufferLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "pub");
		
		EventQueueFactory eventQueueFactory = _concentusHandle.getEventQueueFactory();
		
		_inputQueue = eventQueueFactory.createSingleProducerQueue("inputQueue", msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				recvBufferLength, new YieldingWaitStrategy());
		
		_outputQueue = eventQueueFactory.createSingleProducerQueue("pubQueue", msgBufferFactory(MSG_BUFFER_ENTRY_LENGTH), 
				pubBufferLength, new YieldingWaitStrategy());
		
		_pubHeader = new OutgoingEventHeader(0, 2);
		_subHeader = new IncomingEventHeader(0, 2);
		
		_stateLogic = new StateLogic() {

			private int i = 0;
			
			@Override
			public void collectInput(ByteBuffer inputBuffer) {
				i++;
			}

			@Override
			public void tick(long simTime, long timeDelta) {
				i += 2;
			}

			@Override
			public void createUpdate(ByteBuffer updateBuffer) {
				updateBuffer.putInt(i);
			}
			
		};
		
		/*
		 * Configure sockets
		 */
		// sub socket
		int inputPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "input");
		SocketSettings subSocketSettings = SocketSettings.create()
				.bindToPort(inputPort)
				.setHWM(1000)
				.subscribeToAll();
		_inputSocketId = _socketManager.create(ZMQ.SUB, subSocketSettings, "input");
		
		// pub socket
		int pubPort = ConfigurationUtil.getPort(config, SERVICE_TYPE, "pub");
		SocketSettings pubSocketSettings = SocketSettings.create()
				.bindToPort(pubPort)
				.setHWM(1000);
		_pubSocketId = _socketManager.create(ZMQ.PUB, pubSocketSettings, "pub");
	}

	@Override
	public void onStateChanged(ConcentusServiceState newClusterState,
			ClusterWorkerHandle cluster) throws Exception {
		LOG.info(String.format("Entering state %s", newClusterState.name()));
		switch (newClusterState) {
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
	
	private void onBind(ClusterWorkerHandle cluster) throws Exception {
		// infrastructure for sub socket
		Mutex<Messenger> subSocketPackageMutex = _socketManager.getSocketMutex(_inputSocketId);
		_subListener = Util.asStateful(new EventListener("inputListener", _subHeader, subSocketPackageMutex, _inputQueue, _concentusHandle));
		
		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader> pubSendQueue = new SendQueue<>("publisher", _pubHeader, _outputQueue);
		Mutex<Messenger> pubSocketPackageMutex = _socketManager.getSocketMutex(_pubSocketId);
		_publisher = MessagingUtil.asSocketOwner("publisher", _outputQueue, new Publisher(_pubHeader), pubSocketPackageMutex);
		
		_stateProcessor = new StateProcessor(_concentusHandle.getClock(), _stateLogic, _subHeader, pubSendQueue, _metricContext);
		
		_pipeline = ProcessingPipeline.<byte[]>build(_subListener, _concentusHandle.getClock())
				.thenConnector(_inputQueue)
				.then(_inputQueue.createEventProcessor("stateProcessor", _stateProcessor, _concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_outputQueue)
				.then(_publisher)
				.createPipeline(_executor);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _concentusHandle.getNetworkAddress().getHostAddress()));
	}


	private void onStart(ClusterWorkerHandle cluster) {
		_pipeline.start();
	}

	private void onShutdown(ClusterWorkerHandle cluster) throws Exception {
		_pipeline.halt(60, TimeUnit.SECONDS);
		_socketManager.close();
	}

	@Override
	public Class<ConcentusServiceState> getStateValueClass() {
		return ConcentusServiceState.class;
	}
	
}
