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
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
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
	private volatile Thread _processorThread = null;
	
	private final NonBlockingRingBufferWriter<byte[]> _recvBufferWriter;
	private final NonBlockingRingBufferReader<byte[]> _sendBufferReader;
	private final FatalExceptionCallback _exCallback;
	
	private EventLogic<?> _eventLogic;
	
	public SendRecvSocketReactor(
			final NonBlockingRingBufferWriter<byte[]> recvBufferWriter,
			final NonBlockingRingBufferReader<byte[]> sendBufferReader,
			final FatalExceptionCallback exCallback) {
		_recvBufferWriter = Objects.requireNonNull(recvBufferWriter);
		_sendBufferReader = Objects.requireNonNull(sendBufferReader);
		_exCallback = Objects.requireNonNull(exCallback);
	}
	
	public void configure(final Mutex<SocketPackage> socketPackageMutex, 
			final OutgoingEventHeader sendHeader, 
			final IncomingEventHeader recvHeader) {
		Objects.requireNonNull(socketPackageMutex);
		socketPackageMutex.runAsOwner(new OwnerDelegate<SocketPackage>() {

			@Override
			public void asOwner(SocketPackage item) {
				ZMQ.Socket socket = item.getSocket();
				if (socket.getType() != ZMQ.ROUTER && socket.getType() != ZMQ.DEALER) {
					throw new IllegalArgumentException("Only router and dealer sockets are supported by this reactor");
				}
			}
			
		});
		
		Objects.requireNonNull(recvHeader);
		Objects.requireNonNull(sendHeader);
		
		_eventLogic = new EventLogic<>(socketPackageMutex, new SocketOperations<SocketPackage>() {

			@Override
			public boolean send(SocketPackage socketItem, byte[] outgoingBuffer) {
				return Messaging.send(socketItem, outgoingBuffer, sendHeader, false);
			}

			@Override
			public boolean recv(SocketPackage socketItem, byte[] incomingBuffer) {
				return Messaging.recv(socketItem, incomingBuffer, recvHeader, false);
			}

			@Override
			public boolean isMultiSocket() {
				return false;
			}

			@Override
			public IncomingEventHeader getRecvHeader() {
				return recvHeader;
			}

		});
	}
	
	public void configure(
			final Mutex<SocketPollInSet> socketPollInSetMutex, 
			final MultiSocketOutgoingEventHeader sendHeader, 
			final IncomingEventHeader recvHeader) {
		Objects.requireNonNull(socketPollInSetMutex);
		socketPollInSetMutex.runAsOwner(new OwnerDelegate<SocketPollInSet>() {

			@Override
			public void asOwner(SocketPollInSet item) {
				for (SocketPackage socketPackage : item.getSockets()) {
					ZMQ.Socket socket = socketPackage.getSocket();
					if (socket.getType() != ZMQ.ROUTER && socket.getType() != ZMQ.DEALER) {
						throw new IllegalArgumentException("Only router and dealer sockets are supported by this reactor");
					}
				}
			}
			
		});
		Objects.requireNonNull(recvHeader);
		Objects.requireNonNull(sendHeader);
		
		_eventLogic = new EventLogic<>(socketPollInSetMutex, new SocketOperations<SocketPollInSet>() {

			@Override
			public boolean send(SocketPollInSet socketItem,
					byte[] outgoingBuffer) {
				return Messaging.send(socketItem, outgoingBuffer, sendHeader, false);
			}

			@Override
			public boolean recv(SocketPollInSet socketItem,
					byte[] incomingBuffer) {
				return Messaging.recv(socketItem, incomingBuffer, recvHeader, false);
			}

			@Override
			public boolean isMultiSocket() {
				return true;
			}

			@Override
			public IncomingEventHeader getRecvHeader() {
				return recvHeader;
			}

		});
	}
	
	public boolean isMultiSocket() {
		return _eventLogic.isMultiSocket();
	}
	
	@Override
	public void run() {
		_eventLogic.run();
	}
	
	@Override
	public Sequence getSequence() {
		return _sendBufferReader.getSequence();
	}

	@Override
	public void halt() {
		boolean wasRunning = _running.getAndSet(false);
		_sendBufferReader.getBarrier().alert();
		Thread processorThread = _processorThread;
		if (wasRunning && processorThread != null) {
			_processorThread.interrupt();
		}
	}
	
	private interface SocketOperations<TSocketItem> {
		boolean send(TSocketItem socketItem, byte[] outgoingBuffer);
		boolean recv(TSocketItem socketItem, byte[] incomingBuffer);
		boolean isMultiSocket();
		IncomingEventHeader getRecvHeader();
	}
	
	private class EventLogic<TSocketItem> implements Runnable {

		private final Mutex<TSocketItem> _socketItemMutex;
		private final SocketOperations<TSocketItem> _socketOperations;
		
		public EventLogic(final Mutex<TSocketItem> socketItemMutex, final SocketOperations<TSocketItem> socketOperations) {
			_socketItemMutex = Objects.requireNonNull(socketItemMutex);
			_socketOperations = Objects.requireNonNull(socketOperations);
		}
		
		@Override
		public void run() {
			_socketItemMutex.runAsOwner(new OwnerDelegate<TSocketItem>() {
				
				@Override
				public void asOwner(TSocketItem socketItem) {
					if (!_running.compareAndSet(false, true)) {
						throw new IllegalStateException(String.format("The %s can only be started once.", 
								DeadlineBasedEventProcessor.class.getName()));
					}
					_processorThread = Thread.currentThread();
					_sendBufferReader.getBarrier().clearAlert();
					
					try {
						int inactivityCount = 0;
						while(!Thread.interrupted()) {	
							try {
								boolean wasActivity = false;	
								
								byte[] recvBuffer = _recvBufferWriter.claimNoBlock();
								if (recvBuffer != null && _socketOperations.recv(socketItem, recvBuffer)) {
									_recvBufferWriter.publish();
									wasActivity = true;
								}
								
								byte[] sendBuffer = _sendBufferReader.getIfReady();
								if (sendBuffer != null && _socketOperations.send(socketItem, sendBuffer)) {
									_sendBufferReader.advance();
									wasActivity = true;
								}
								
								if (!wasActivity) {
									inactivityCount++;
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
						if (_recvBufferWriter.hasUnpublished()) {
							byte[] unpublishedEvent = _recvBufferWriter.getUnpublished();
							_socketOperations.getRecvHeader().setIsValid(unpublishedEvent, false);
							_recvBufferWriter.publish();
						}
						_processorThread = null;
					}
				}
				
			});
		}
		
		public boolean isMultiSocket() {
			return _socketOperations.isMultiSocket();
		}
		
	}

}
