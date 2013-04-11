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
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventProcessor;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferReader;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferWriter;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * The Router and Dealer 0MQ socket types need to have both send and recv operations
 * invoked on them - this becomes tricky when trying to gate thread
 * access to the socket. This class wraps the given socket with
 * a reactor that accepts events into an incoming {@link RingBuffer}, and sends
 * events from and outgoing {@link RingBuffer}. This makes the socket
 * play well with pipeline processing.
 * 
 * @author Adam Roughton
 *
 */
public class SendRecvSocketReactor implements EventProcessor {
	
	/**
	 * The threshold for yielding the thread after running the send and recv loop
	 * with no activity
	 */
	private static final int INACTIVITY_THRESHOLD = 10;
	
	private final AtomicBoolean _running = new AtomicBoolean(false);
	
	private boolean _isMultiSocket = false;
	private SendOperation _sendOperation;
	private RecvOperation _recvOperation;
	
	private final NonBlockingRingBufferWriter<byte[]> _recvBufferWriter;
	private final NonBlockingRingBufferReader<byte[]> _sendBufferReader;
	private final FatalExceptionCallback _exCallback;
	
	public SendRecvSocketReactor(
			final NonBlockingRingBufferWriter<byte[]> recvBufferWriter,
			final NonBlockingRingBufferReader<byte[]> sendBufferReader,
			final FatalExceptionCallback exCallback) {
		_recvBufferWriter = Objects.requireNonNull(recvBufferWriter);
		_sendBufferReader = Objects.requireNonNull(sendBufferReader);
		_exCallback = Objects.requireNonNull(exCallback);
	}

	public void configure(final SocketPackage socketPackage, final OutgoingEventHeader sendHeader, final IncomingEventHeader recvHeader) throws IllegalStateException {
		if (_running.get()) throw new IllegalStateException("The reactor cannot be configured while running.");
		
		Objects.requireNonNull(socketPackage);
		ZMQ.Socket socket = socketPackage.getSocket();
		if (socket.getType() != ZMQ.ROUTER && socket.getType() != ZMQ.DEALER) {
			throw new IllegalArgumentException("Only router and dealer sockets are supported by this reactor");
		}
		
		Objects.requireNonNull(recvHeader);
		Objects.requireNonNull(sendHeader);
		
		_recvOperation = new RecvOperation() {
			
			public boolean recv(byte[] incomingBuffer) {
				return Messaging.recv(socketPackage, incomingBuffer, recvHeader, false);
			}
			
		};
		_sendOperation = new SendOperation() {

			@Override
			public boolean send(byte[] outgoingBuffer) {
				return Messaging.send(socketPackage, outgoingBuffer, sendHeader, false);
			}
			
		};
	}
	
	public void configure(final SocketPollInSet socketPollInSet, final MultiSocketOutgoingEventHeader sendHeader, final IncomingEventHeader recvHeader) throws IllegalStateException {
		if (_running.get()) throw new IllegalStateException("The reactor cannot be configured while running.");
		
		Objects.requireNonNull(socketPollInSet);
		for (SocketPackage socketPackage : socketPollInSet.getSockets()) {
			ZMQ.Socket socket = socketPackage.getSocket();
			if (socket.getType() != ZMQ.ROUTER && socket.getType() != ZMQ.DEALER) {
				throw new IllegalArgumentException("Only router and dealer sockets are supported by this reactor");
			}
		}
		
		Objects.requireNonNull(recvHeader);
		Objects.requireNonNull(sendHeader);
		
		_recvOperation = new RecvOperation() {
			
			public boolean recv(byte[] incomingBuffer) {
				return Messaging.recv(socketPollInSet, incomingBuffer, recvHeader, false);
			}
			
		};
		_sendOperation = new SendOperation() {

			@Override
			public boolean send(byte[] outgoingBuffer) {
				return Messaging.send(socketPollInSet, outgoingBuffer, sendHeader, false);
			}
			
		};
	}
	
	public boolean isMultiSocket() {
		return _isMultiSocket;
	}
	
	@Override
	public void run() {
		if (!_running.compareAndSet(false, true)) {
			throw new IllegalStateException(String.format("The %s can only be started once.", 
					DeadlineBasedEventProcessor.class.getName()));
		}
		_sendBufferReader.getBarrier().clearAlert();
		
		try {
			int inactivityCount = 0;
			while(true) {	
				try {
					boolean wasActivity = false;	
					
					byte[] recvBuffer = _recvBufferWriter.claimNoBlock();
					if (recvBuffer != null && _recvOperation.recv(recvBuffer)) {
						_recvBufferWriter.publish();
						wasActivity &= true;
					}
					
					byte[] sendBuffer = _sendBufferReader.getIfReady();
					if (sendBuffer != null && _sendOperation.send(sendBuffer)) {
						_sendBufferReader.advance();
						wasActivity &= true;
					}
					
					if (!wasActivity) {
						inactivityCount--;
					}
					if (inactivityCount >= INACTIVITY_THRESHOLD) {
						Thread.yield();
						inactivityCount = 0;
					}
				} catch (final AlertException eAlert) {
					if (!_running.get()) {
						break;
					}
				}
			}
		} catch (Throwable e) {
			_exCallback.signalFatalException(e);
		} finally {
			_recvBufferWriter.tidyUp();
		}
	}
	
	@Override
	public Sequence getSequence() {
		return _sendBufferReader.getSequence();
	}

	@Override
	public void halt() {
		_running.set(false);
		_sendBufferReader.getBarrier().alert();
	}
	
	private interface SendOperation {
		boolean send(byte[] outgoingBuffer);
	}
	
	private interface RecvOperation {
		boolean recv(byte[] incomingBuffer);
	}

}
