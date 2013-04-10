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

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.lmax.disruptor.RingBuffer;

public final class EventListener implements Runnable {
	
	private final IncomingEventHeader _header;
	private final RingBuffer<byte[]> _ringBuffer;
	private final FatalExceptionCallback _exCallback;
	private ListenerLogic _listenerLogic;
		
	public EventListener(
			final IncomingEventHeader header,
			final SocketPackage socketPackage,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		this(header, ringBuffer, exCallback);
		_listenerLogic = new SingleSocketListener(socketPackage);
	}
	
	public EventListener(
			final IncomingEventHeader header,
			final SocketPollInSet pollInSet,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		this(header, ringBuffer, exCallback);
		_listenerLogic = new MultiSocketListener(pollInSet);
	}
	
	private EventListener(
			final IncomingEventHeader header,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		_header = Objects.requireNonNull(header);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {	
		try {
			try {		
				_listenerLogic.listen();
			} catch (ZMQException eZmq) {
				// check that the socket hasn't just been closed
				if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
					throw eZmq;
				}
			} 
		} catch (Throwable t) {
			_exCallback.signalFatalException(t);
		}
	}
	
	private void nextEvent(final SocketPackage socketPackage) {
		final long seq = _ringBuffer.next();
		final byte[] incomingBuffer = _ringBuffer.get(seq);
		try {
			Messaging.recv(socketPackage, incomingBuffer, _header, true);
		} finally {
			_ringBuffer.publish(seq);
		}
	}
	
	private interface ListenerLogic {
		void listen();
	}
	
	private class SingleSocketListener implements ListenerLogic {

		private final SocketPackage _socketPackage;
		
		public SingleSocketListener(final SocketPackage socketPackage) {
			_socketPackage = socketPackage;
		}
		
		@Override
		public void listen() {
			while (!Thread.interrupted()) {
				nextEvent(_socketPackage);
			}
		}
	}
	
	private class MultiSocketListener implements ListenerLogic {

		private final SocketPollInSet _pollInSet;
		
		public MultiSocketListener(final SocketPollInSet pollInSet) {
			_pollInSet = pollInSet;
		}
		
		@Override
		public void listen() {
			try {
				for (;;) {
					nextEvent(_pollInSet.poll());
				}
			} catch (InterruptedException eInterrupted) {
				Thread.currentThread().interrupt();
			}
		}
		
	}
	
}
