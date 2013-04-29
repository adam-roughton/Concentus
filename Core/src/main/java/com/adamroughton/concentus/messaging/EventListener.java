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
package com.adamroughton.concentus.messaging;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.messaging.SocketMutex.SocketSetMutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.lmax.disruptor.RingBuffer;

public final class EventListener implements Runnable {
	
	private final IncomingEventHeader _header;
	private final RingBuffer<byte[]> _ringBuffer;
	private final FatalExceptionCallback _exCallback;
	private ListenerLogic _listenerLogic;
		
	public EventListener(
			final IncomingEventHeader header,
			final SocketMutex socketToken,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		this(header, ringBuffer, exCallback);
		_listenerLogic = new SingleSocketListener(socketToken);
	}
	
	public EventListener(
			final IncomingEventHeader header,
			final SocketSetMutex<SocketPollInSet> pollInSetToken,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		this(header, ringBuffer, exCallback);
		_listenerLogic = new MultiSocketListener(pollInSetToken);
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
			if (!Messaging.recv(socketPackage, incomingBuffer, _header, true)) {
				_header.setIsValid(incomingBuffer, false);
			}
		} finally {
			_ringBuffer.publish(seq);
		}
	}
	
	private interface ListenerLogic {
		void listen();
	}
	
	private class SingleSocketListener implements ListenerLogic {

		private final SocketMutex _socketToken;
		
		public SingleSocketListener(final SocketMutex socketToken) {
			_socketToken = socketToken;
		}
		
		@Override
		public void listen() {
			_socketToken.runAsOwner(new OwnerDelegate<SocketPackage>() {
				
				@Override
				public void asOwner(SocketPackage socketPackage) {
					while (!Thread.interrupted()) {
						nextEvent(socketPackage);
					}
				}
			});
		}
	}
	
	private class MultiSocketListener implements ListenerLogic {

		private final SocketSetMutex<SocketPollInSet> _pollInSetToken;
		
		public MultiSocketListener(final SocketSetMutex<SocketPollInSet> pollInSetToken) {
			_pollInSetToken = pollInSetToken;
		}
		
		@Override
		public void listen() {
			_pollInSetToken.runAsOwner(new OwnerDelegate<SocketPollInSet>() {

				@Override
				public void asOwner(SocketPollInSet pollInSet) {
					try {
						while(!Thread.interrupted()) {
							nextEvent(pollInSet.poll());
						}
					} catch (InterruptedException eInterrupted) {
						Thread.currentThread().interrupt();
					}
				}
			});
			
		}
		
	}
	
}
