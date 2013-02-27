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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.Util;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

/**
 * Manages a 0MQ Router socket that communicates with all 
 * connected clients for this client handler. As we need 
 * to both send and receive on the same socket, this class 
 * fairly multiplexes send and receive operations.
 * 
 * @author Adam Roughton
 *
 */
public class ClientEventHandler implements Runnable {

	private final int _clientPort;
	private final ZMQ.Context _zmqContext;
	private final AtomicBoolean _hasStarted = new AtomicBoolean(false);
	
	private final IncomingClientEventHandler _incomingEventHandler;
	private final OutgoingClientEventHandler _outgoingEventHandler;
		
	private final FatalExceptionCallback _exCallback;
	
	public ClientEventHandler(
			final int clientPort, 
			final ZMQ.Context zmqContext,
			final RingBuffer<byte[]> incomingRingBuffer,
			final RingBuffer<byte[]> outgoingRingBuffer,
			final SequenceBarrier outgoingBarrier,
			final FatalExceptionCallback exCallback) {
		Util.assertPortValid(clientPort);
		_clientPort = clientPort;
		_zmqContext = Objects.requireNonNull(zmqContext);
		
		_incomingEventHandler = new IncomingClientEventHandler(incomingRingBuffer);
		_outgoingEventHandler = new OutgoingClientEventHandler(outgoingRingBuffer, outgoingBarrier);
		
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {
		if (!_hasStarted.compareAndSet(false, true)) {
			_exCallback.signalFatalException(new RuntimeException(
					"The client event handler has already been started."));
		}
		try {
			ZMQ.Socket socket = _zmqContext.socket(ZMQ.ROUTER);
			socket.bind(String.format("tcp://*:%d", _clientPort));
			
			boolean wasActivity = false;
			while(!Thread.interrupted()) {				
				wasActivity &= _incomingEventHandler.recvIfReady(socket);
				wasActivity &= _outgoingEventHandler.sendIfReady(socket);
				if (!wasActivity) {
					// yield the thread
					LockSupport.parkNanos(1L);
				}
			}
		} catch (Throwable e) {
			_exCallback.signalFatalException(e);
		} finally {
			_incomingEventHandler.tidyUp();
		}
	}
	
	public Sequence getIncomingSequence() {
		return _incomingEventHandler.getSequence();
	}
	
}
