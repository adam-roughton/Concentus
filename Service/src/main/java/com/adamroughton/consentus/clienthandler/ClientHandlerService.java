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
package com.adamroughton.consentus.clienthandler;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.canonicalstate.StateLogic;
import com.adamroughton.consentus.canonicalstate.StateProcessor;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.adamroughton.consentus.messaging.EventListener;
import com.adamroughton.consentus.messaging.SocketSettings;
import com.adamroughton.consentus.messaging.SubSocketSettings;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class ClientHandlerService implements ConsentusService {

	private ExecutorService _executor;
	private Disruptor<byte[]> _inputDisruptor;
	private Disruptor<byte[]> _outputDisruptor;
	
	private EventListener _eventListener;
	private StateProcessor _stateProcessor;
	private Publisher _publisher;	
	
	private ZMQ.Context _zmqContext;
	
	private int _clientHandlerId;
	
	@Override
	public void start(final Config config, final ConsentusProcessCallback exHandler) {

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
		return String.format("Client Handler %d", _clientHandlerId);
	}


	
	
	
	
	
}
