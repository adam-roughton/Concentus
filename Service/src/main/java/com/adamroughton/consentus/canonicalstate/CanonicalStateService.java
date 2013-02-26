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

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.SubSocketSettings;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class CanonicalStateService implements ConsentusService {
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _inputDisruptor;
	private Disruptor<byte[]> _outputDisruptor;
	
	private EventListener _eventListener;
	private StateProcessor _stateProcessor;
	private Publisher _publisher;	
	
	private ZMQ.Context _zmqContext;

	@SuppressWarnings("unchecked")
	@Override
	public void start(Config config, ConsentusProcessCallback exHandler) {
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
				_outputDisruptor.getRingBuffer(), stateProcBarrier, exHandler, config);
		_inputDisruptor.handleEventsWith(_stateProcessor);
		
		_publisher = new Publisher(_zmqContext, config);
		_outputDisruptor.handleEventsWith(_publisher);
		
		_outputDisruptor.start();
		_inputDisruptor.start();
		
		int listenPort = Util.getPort(config.getCanonicalSubPort());
		SocketSettings socketSettings = SocketSettings.create(ZMQ.SUB)
				.bindToPort(listenPort)
				.setMessageOffsets(0, 0);
		SubSocketSettings subSocketSettings = SubSocketSettings.create(socketSettings)
				.subscribeToAll();
		
		_eventListener = new EventListener(subSocketSettings, _inputDisruptor.getRingBuffer(), _zmqContext, exHandler);
		_executor.submit(_eventListener);
	}
	
	@Override
	public void shutdown() {
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
		return "Canonical State Service";
	}
	
}
