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
import org.zeromq.ZMQException;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.RingBuffer;

public final class EventListener implements Runnable {
	private final AtomicBoolean _isRunning = new AtomicBoolean(false);
	
	private final RingBuffer<byte[]> _ringBuffer;
	private final FatalExceptionCallback _exCallback;
	private ListenerLogic _listenerLogic;
		
	public EventListener(
			final SocketPackage socketPackage,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		this(ringBuffer, exCallback);
		_listenerLogic = new SingleSocketListener(socketPackage);
	}
	
	public EventListener(
			final SocketPollInSet pollInSet,
			final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		this(ringBuffer, exCallback);
		_listenerLogic = new MultiSocketListener(pollInSet);
		
	}
	
	private EventListener(final RingBuffer<byte[]> ringBuffer, 
			final FatalExceptionCallback exCallback) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {	
		if (!_isRunning.compareAndSet(false, true)) {
			_exCallback.signalFatalException(new RuntimeException("The event listener can only be started once."));
		}
		
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
		ZMQ.Socket input = socketPackage.getSocket();
		MessageFrameBufferMapping mapping = socketPackage.getMessageFrameBufferMapping();
		int socketId = socketPackage.getSocketId();
		
		final long seq = _ringBuffer.next();
		final byte[] array = _ringBuffer.get(seq);
		try {
			int result = -1;
			// we reserve the first byte of the buffer to communicate
			// whether the event was received correctly
			int offset = 1;
			for (int i = 0; i < mapping.partCount(); i++) {
				offset = mapping.getOffset(i) + 1;
				result = input.recv(array, offset, array.length - offset, 0);
				if (result == -1)
					break;
				if (mapping.partCount() > i + 1 &&					
						!input.hasReceiveMore()) {
					result = -1;
					break;
				}
			}
			if (result == -1) {
				MessageBytesUtil.writeFlagToByte(array, 0, 0, true);
			} else {
				MessageBytesUtil.writeFlagToByte(array, 0, 0, false);
			}		
		} catch (Exception e) {
			// indicate error condition on the message
			MessageBytesUtil.writeFlagToByte(array, 0, 0, true);
			Log.error("An error was raised on receiving a message.", e);
			throw new RuntimeException(e);
		} finally {
			MessageBytesUtil.write4BitUInt(array, 0, 4, socketId);
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
