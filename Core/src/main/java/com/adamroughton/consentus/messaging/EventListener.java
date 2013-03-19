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
	
	private final ZMQ.Context _zmqContext;
	private final RingBuffer<byte[]> _ringBuffer;
	private final FatalExceptionCallback _exCallback;
	
	private final SocketPackage[] _socketPackages;
	
	public EventListener(
			final SocketPackage socketPackage,
			final RingBuffer<byte[]> ringBuffer, 
			final ZMQ.Context zmqContext,
			final FatalExceptionCallback exCallback) {
		this(new SocketPackage[] {socketPackage}, ringBuffer, zmqContext, exCallback);
	}
	
	public EventListener(
			final SocketPackage[] socketPackages,
			final RingBuffer<byte[]> ringBuffer, 
			final ZMQ.Context zmqContext,
			final FatalExceptionCallback exCallback) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_zmqContext = Objects.requireNonNull(zmqContext);
		_exCallback = Objects.requireNonNull(exCallback);
		_socketPackages = Objects.requireNonNull(socketPackages);
	}

	@Override
	public void run() {	
		if (!_isRunning.compareAndSet(false, true)) {
			_exCallback.signalFatalException(new RuntimeException("The event listener can only be started once."));
		}
		
		try {
			try {		
				if (_socketPackages.length == 1) {
					doRecvSingleSocket(_socketPackages[0]);
				} else {
					doRecvMultiSocket(_socketPackages);
				}
			} catch (ZMQException eZmq) {
				// check that the socket hasn't just been closed
				if (eZmq.getErrorCode() != ZMQ.Error.ETERM.getCode()) {
					throw eZmq;
				}
			} finally {
				try {
					for (int i = 0; i < _socketPackages.length; i++) {
						_socketPackages[i].getSocket().close();
					}
				} catch (Exception eClose) {
					Log.warn("Exception thrown when closing ZMQ socket.", eClose);
				}
			}
		} catch (Throwable t) {
			_exCallback.signalFatalException(t);
		}
	}
	
	private void doRecvSingleSocket(final SocketPackage socketPackage) throws Exception {
		while (!Thread.interrupted()) {
			nextEvent(socketPackage.getSocket(), 
					socketPackage.getMessagePartPolicy(), 
					socketPackage.getSocketId());
		}
	}
	
	private void doRecvMultiSocket(SocketPackage[] socketPackages) throws Exception {
		ZMQ.Poller poller = _zmqContext.poller(socketPackages.length);
		for (int i = 0; i < socketPackages.length; i++) {
			poller.register(socketPackages[i].getSocket(), ZMQ.Poller.POLLIN);
		}
		while (!Thread.interrupted()) {
			poller.poll();
			for (int i = 0; i < socketPackages.length; i++) {
				if (poller.pollin(i)) {
					nextEvent(poller.getSocket(i), 
							socketPackages[i].getMessagePartPolicy(), 
							socketPackages[i].getSocketId());
				}
			}
		}
	}
	
	private void nextEvent(final ZMQ.Socket input, final MessagePartBufferPolicy msgPartPolicy, final int socketId) {
		final long seq = _ringBuffer.next();
		final byte[] array = _ringBuffer.get(seq);
		try {
			int result = -1;
			// we reserve the first byte of the buffer to communicate
			// whether the event was received correctly
			int offset = 1;
			for (int i = 0; i < msgPartPolicy.partCount(); i++) {
				offset = msgPartPolicy.getOffset(i) + 1;
				result = input.recv(array, offset, array.length - offset, 0);
				if (result == -1)
					break;
				if (msgPartPolicy.partCount() > i + 1 &&					
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
}
