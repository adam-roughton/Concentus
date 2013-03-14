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
	
	private final AtomicBoolean _hasStarted = new AtomicBoolean(false);
	
	private final SocketPackage _socketPackage;		
	private final NonblockingEventReceiver _receiver;
	private final NonblockingEventSender _sender;
	private final FatalExceptionCallback _exCallback;
	
	public RouterSocketReactor(
			final SocketPackage socketPackage,
			final NonblockingEventReceiver receiver,
			final NonblockingEventSender sender,
			final FatalExceptionCallback exCallback) {
		_socketPackage = Objects.requireNonNull(socketPackage);
		ZMQ.Socket socket = _socketPackage.getSocket();
		if (socket.getType() != ZMQ.ROUTER) {
			throw new IllegalArgumentException("Only router sockets are supported by this reactor");
		}
		
		_receiver = Objects.requireNonNull(receiver);
		_sender = Objects.requireNonNull(sender);
		
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {
		if (!_hasStarted.compareAndSet(false, true)) {
			_exCallback.signalFatalException(new RuntimeException(
					"The router socket reactor has already been started."));
		}
		try {
			int inactivityCount = 0;
			while(!Thread.interrupted()) {	
				boolean wasActivity = false;
				wasActivity &= _receiver.recvIfReady(_socketPackage);
				wasActivity &= _sender.sendIfReady(_socketPackage);
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
