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
package com.adamroughton.consentus.messaging;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import static com.adamroughton.consentus.Util.*;

/**
 * 0MQ router sockets need to have both send and recv operations
 * invoked on them - this becomes tricky when trying to gate thread
 * access to the socket. This class wraps the router socket with
 * a reactor that accepts events into an incoming {@link RingBuffer}, and sends
 * events from and outgoing {@link RingBuffer}. This makes the socket
 * play well with pipeline processing.
 * 
 * @author Adam Roughton
 *
 */
public class RouterSocketReactor implements Runnable {
	
	/**
	 * The threshold for yielding the thread after running the send and recv loop
	 * with no activity
	 */
	private static final int INACTIVITY_THRESHOLD = 10;
	
	private final ZMQ.Context _zmqContext;
	private final AtomicBoolean _hasStarted = new AtomicBoolean(false);
	
	private final NonblockingEventReceiver _receiver;
	private final NonblockingEventSender _sender;
	
	private final SocketSettings _socketSettings;
		
	private final FatalExceptionCallback _exCallback;
	
	public RouterSocketReactor(
			final SocketSettings socketSettings,
			final ZMQ.Context zmqContext,
			final RingBuffer<byte[]> incomingRingBuffer,
			final RingBuffer<byte[]> outgoingRingBuffer,
			final SequenceBarrier outgoingBarrier,
			final EventProcessingHeader processingHeader,
			final FatalExceptionCallback exCallback) {
		_socketSettings = Objects.requireNonNull(socketSettings);
		if (_socketSettings.getSocketType() != ZMQ.ROUTER) {
			throw new IllegalArgumentException("Only router sockets are supported by this reactor");
		}
		_zmqContext = Objects.requireNonNull(zmqContext);
		
		_receiver = new NonblockingEventReceiver(incomingRingBuffer, processingHeader);
		_sender = new NonblockingEventSender(outgoingRingBuffer, outgoingBarrier, processingHeader);
		
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {
		if (!_hasStarted.compareAndSet(false, true)) {
			_exCallback.signalFatalException(new RuntimeException(
					"The router socket reactor has already been started."));
		}
		try {
			ZMQ.Socket socket = createSocket(_zmqContext, _socketSettings);
			MessagePartBufferPolicy msgPartPolicy = _socketSettings.getMessagePartPolicy();
			int socketId = _socketSettings.getSocketId();
			
			int inactivityCount = 0;
			while(!Thread.interrupted()) {	
				boolean wasActivity = false;
				wasActivity &= _receiver.recvIfReady(socket, msgPartPolicy, socketId);
				wasActivity &= _sender.sendIfReady(socket, msgPartPolicy);
				if (!wasActivity) {
					inactivityCount--;
				}
				if (inactivityCount >= INACTIVITY_THRESHOLD) {
					Thread.yield();
					inactivityCount = 0;
				}
			}
		} catch (Throwable e) {
			_exCallback.signalFatalException(e);
		} finally {
			_receiver.tidyUp();
		}
	}
	
	public Sequence getIncomingSequence() {
		return _receiver.getSequence();
	}
	
	public Sequence getOutgoingSequence() {
		return _sender.getSequence();
	}
	
}
