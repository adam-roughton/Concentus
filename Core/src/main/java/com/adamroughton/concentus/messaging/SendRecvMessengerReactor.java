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

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventProcessor;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.disruptor.EventQueueReader;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * Some messengers are backed by end points that need to have both send and recv operations
 * invoked on them - this becomes tricky when trying to gate thread
 * access to the messenger. This class wraps the given messenger with
 * a reactor that accepts events into an incoming {@link RingBuffer}, and sends
 * events from and outgoing {@link RingBuffer}. This makes the messenger
 * play well with pipeline processing.
 * 
 * @author Adam Roughton
 *
 */
public final class SendRecvMessengerReactor implements EventProcessor {
	
	/**
	 * The threshold for yielding the thread after running the send and recv loop
	 * with no activity
	 */
	private static final int INACTIVITY_THRESHOLD = 10;
	
	private final AtomicBoolean _running = new AtomicBoolean(false);
	private volatile Thread _processorThread = null;
	
	private final Mutex<Messenger> _messengerMutex;
	private final OutgoingEventHeader _sendHeader;
	private final IncomingEventHeader _recvHeader;
	private final EventQueuePublisher<byte[]> _recvQueuePublisher;
	private final EventQueueReader<byte[]> _sendQueueReader;
	private final FatalExceptionCallback _exCallback;
	
	public SendRecvMessengerReactor(
			Mutex<Messenger> messengerMutex,
			OutgoingEventHeader sendHeader,
			IncomingEventHeader recvHeader,
			EventQueue<byte[]> recvQueue,
			EventQueue<byte[]> sendQueue,
			FatalExceptionCallback exCallback) {
		_messengerMutex = Objects.requireNonNull(messengerMutex);
		_sendHeader = Objects.requireNonNull(sendHeader);
		_recvHeader = Objects.requireNonNull(recvHeader);
		_recvQueuePublisher = recvQueue.createPublisher(false);
		_sendQueueReader = sendQueue.createReader(false);
		_exCallback = Objects.requireNonNull(exCallback);
	}
	
	@Override
	public void run() {
		_messengerMutex.runAsOwner(new OwnerDelegate<Messenger>() {
			
			@Override
			public void asOwner(Messenger messenger) {
				if (!_running.compareAndSet(false, true)) {
					throw new IllegalStateException(String.format("The %s can only be started once.", 
							DeadlineBasedEventProcessor.class.getName()));
				}
				_processorThread = Thread.currentThread();
				_sendQueueReader.getBarrier().clearAlert();
				
				try {
					int inactivityCount = 0;
					while(!Thread.interrupted()) {	
						try {
							boolean wasActivity = false;	
							
							byte[] recvBuffer = _recvQueuePublisher.next();
							if (recvBuffer != null && messenger.recv(recvBuffer, _recvHeader, false)) {
								_recvQueuePublisher.publish();
								wasActivity = true;
							}
							
							byte[] sendBuffer = _sendQueueReader.get();
							if (sendBuffer != null && messenger.send(sendBuffer, _sendHeader, false)) {
								_sendQueueReader.advance();
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
					if (_recvQueuePublisher.hasUnpublished()) {
						byte[] unpublishedEvent = _recvQueuePublisher.next();
						_recvHeader.setIsValid(unpublishedEvent, false);
						_recvQueuePublisher.publish();
					}
					_processorThread = null;
				}
			}
			
		});
	}
	
	@Override
	public Sequence getSequence() {
		return _sendQueueReader.getSequence();
	}

	@Override
	public void halt() {
		boolean wasRunning = _running.getAndSet(false);
		_sendQueueReader.getBarrier().alert();
		Thread processorThread = _processorThread;
		if (wasRunning && processorThread != null) {
			processorThread.interrupt();
		}
	}

}
