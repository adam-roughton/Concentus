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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.adamroughton.concentus.ConcentusProcessCallback;
import com.adamroughton.concentus.ConsentusService;
import com.adamroughton.concentus.ConsentusServiceState;
import com.adamroughton.concentus.StatefulRunnable;
import com.adamroughton.concentus.Util;
import com.adamroughton.concentus.cluster.worker.Cluster;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventProcessor;
import com.adamroughton.concentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.SocketManager;
import com.adamroughton.concentus.messaging.SocketPackage;
import com.adamroughton.concentus.messaging.SocketSettings;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

import static com.adamroughton.concentus.Constants.MSG_BUFFER_LENGTH;
import static com.adamroughton.concentus.Util.msgBufferFactory;

public class CanonicalStateService implements ConsentusService {
	
	public final static String SERVICE_TYPE = "CanonicalState";
	private final static Logger LOG = Logger.getLogger(SERVICE_TYPE);
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final SocketManager _socketManager;
	private final Disruptor<byte[]> _inputDisruptor;
	private final Disruptor<byte[]> _outputDisruptor;
	private final OutgoingEventHeader _pubHeader;
	private final IncomingEventHeader _subHeader;
	private final StateLogic _stateLogic;
	
	private ConcentusProcessCallback _exCallback;
	private InetAddress _networkAddress;
	
	private StatefulRunnable<EventListener> _subListener;
	private StateProcessor _stateProcessor;
	private Publisher _publisher;	
	
	private int _pubSocketId;
	private int _subSocketId;
	
	public CanonicalStateService() {
		_socketManager = new SocketManager();
		
		_inputDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		
		_outputDisruptor = new Disruptor<>(msgBufferFactory(MSG_BUFFER_LENGTH), 
				_executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		
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
	}
	
	@Override
	public String name() {
		return "Canonical State Service";
	}

	@Override
	public void configure(Configuration config, ConcentusProcessCallback exHandler, InetAddress networkAddress) {
		_exCallback = exHandler;
		_networkAddress = networkAddress;
		
		_inputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Input Disruptor", exHandler));
		_outputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Output Disruptor", exHandler));
		
		/*
		 * Configure sockets
		 */
		// sub socket
		int subPort = config.getServices().get(SERVICE_TYPE).getPorts().get("sub");
		SocketSettings subSocketSettings = SocketSettings.create()
				.bindToPort(subPort)
				.setHWM(1000)
				.subscribeToAll();
		_subSocketId = _socketManager.create(ZMQ.SUB, subSocketSettings);
		
		// pub socket
		int pubPort = config.getServices().get(SERVICE_TYPE).getPorts().get("pub");
		SocketSettings pubSocketSettings = SocketSettings.create()
				.bindToPort(pubPort)
				.setHWM(1000);
		_pubSocketId = _socketManager.create(ZMQ.PUB, pubSocketSettings);
	}

	@Override
	public void onStateChanged(ConsentusServiceState newClusterState,
			Cluster cluster) throws Exception {
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
	
	@SuppressWarnings("unchecked")
	private void onBind(Cluster cluster) throws Exception {
		_socketManager.bindBoundSockets();
		
		// infrastructure for sub socket
		SocketPackage subSocketPackage = _socketManager.createSocketPackage(_subSocketId);
		_subListener = Util.asStateful(new EventListener(_subHeader, subSocketPackage, _inputDisruptor.getRingBuffer(), _exCallback));
		_socketManager.addDependency(_subSocketId, _subListener);
		
		// infrastructure for pub socket
		SendQueue<OutgoingEventHeader> pubSendQueue = new SendQueue<>(_pubHeader, _outputDisruptor);
		SocketPackage pubSocketPackage = _socketManager.createSocketPackage(_pubSocketId);
		_publisher = new Publisher(pubSocketPackage, _pubHeader);
		
		SequenceBarrier inputBarrier = _inputDisruptor.getRingBuffer().newBarrier();
		_stateProcessor = new StateProcessor(_stateLogic, _subHeader, pubSendQueue);
		
		_inputDisruptor.handleEventsWith(new DeadlineBasedEventProcessor<byte[]>(
				_stateProcessor, _inputDisruptor.getRingBuffer(), inputBarrier, _exCallback));
		_outputDisruptor.handleEventsWith(_publisher);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _networkAddress.getHostAddress()));
	}


	private void onStart(Cluster cluster) {
		_outputDisruptor.start();
		_inputDisruptor.start();
		_executor.submit(_subListener);
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

	@Override
	public Class<ConsentusServiceState> getStateValueClass() {
		return ConsentusServiceState.class;
	}
	
}