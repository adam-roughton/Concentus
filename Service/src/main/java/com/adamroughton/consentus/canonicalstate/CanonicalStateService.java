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
package com.adamroughton.consentus.canonicalstate;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusServiceState;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.config.Configuration;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.Publisher;
import com.adamroughton.consentus.messaging.SocketPackage;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class CanonicalStateService implements ConsentusService {
	
	public final static String SERVICE_TYPE = "CanonicalState";
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _inputDisruptor;
	private Disruptor<byte[]> _outputDisruptor;
	
	private InetAddress _networkAddress;
	
	private EventListener _eventListener;
	private StateProcessor _stateProcessor;
	private Publisher _publisher;	
	
	private ZMQ.Context _zmqContext;
	
	private SocketSettings _subSocketSettings;
	private SocketSettings _pubSocketSettings;
	
	private ZMQ.Socket _pubSocket;
	private ZMQ.Socket _subSocket;
	
	@Override
	public String name() {
		return "Canonical State Service";
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Configuration config, ConsentusProcessCallback exHandler, InetAddress networkAddress) {
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
		
		_outputDisruptor = new Disruptor<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
			
		}, _executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		_outputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Output Disruptor", exHandler));
		
		StateLogic testLogic = new StateLogic() {

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
		
		SequenceBarrier stateProcBarrier = _inputDisruptor.getRingBuffer().newBarrier();
		_stateProcessor = new StateProcessor(testLogic, _inputDisruptor.getRingBuffer(), 
				_outputDisruptor.getRingBuffer(), stateProcBarrier, exHandler);
		_inputDisruptor.handleEventsWith(_stateProcessor);
		
		SocketPackage pubSocketPackage = SocketPackage.create(_pubSocket)
				.setMessageOffsets(0, 4);
		
		_subSocket = _zmqContext.socket(ZMQ.SUB);
		_pubSocket = _zmqContext.socket(ZMQ.PUB);
		
		int subPort = config.getServices().get(SERVICE_TYPE).getPorts().get("sub");
		int pubPort = config.getServices().get(SERVICE_TYPE).getPorts().get("pub");
		
		_subSocketSettings = SocketSettings.create()
				.bindToPort(subPort)
				.setHWM(1000)
				.subscribeToAll();
		_pubSocketSettings = SocketSettings.create()
				.bindToPort(pubPort)
				.setHWM(1000);
		
		SocketPackage subSocketPackage = SocketPackage.create(_subSocket)
				.setMessageOffsets(0, 0);

		_publisher = new Publisher(pubSocketPackage, new EventProcessingHeader(0, 1));
		_outputDisruptor.handleEventsWith(_publisher);
		_eventListener = new EventListener(subSocketPackage, _inputDisruptor.getRingBuffer(), _zmqContext, exHandler);
	}

	private void bind(Cluster cluster) throws Exception {
		_subSocketSettings.configureSocket(_subSocket);
		_pubSocketSettings.configureSocket(_pubSocket);
		
		cluster.registerService(SERVICE_TYPE, String.format("tcp://%s", _networkAddress.getHostAddress()));
	}


	private void start(Cluster cluster) {
		_outputDisruptor.start();
		_inputDisruptor.start();
		_executor.submit(_eventListener);
	}

	private void shutdown(Cluster cluster) {
		_zmqContext.term();
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
	}

	@Override
	public void onStateChanged(ConsentusServiceState newClusterState,
			Cluster cluster) throws Exception {
		switch (newClusterState) {
			case BIND:
				bind(cluster);
				break;
			case START:
				start(cluster);
				break;
			case SHUTDOWN:
				shutdown(cluster);
				break;
			default:
		}
		cluster.signalReady();
	}

	@Override
	public Class<ConsentusServiceState> getStateValueClass() {
		return ConsentusServiceState.class;
	}
	
}
