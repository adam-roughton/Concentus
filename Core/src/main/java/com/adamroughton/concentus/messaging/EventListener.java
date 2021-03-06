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

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;

public final class EventListener<TBuffer extends ResizingBuffer> implements Runnable {
	
	private final IncomingEventHeader _header;
	private final EventQueuePublisher<TBuffer> _recvQueuePublisher;
	private final FatalExceptionCallback _exCallback;
	private final Mutex<Messenger<TBuffer>> _messengerMutex;
	
	public EventListener(
			String name,
			IncomingEventHeader header,
			Mutex<Messenger<TBuffer>> messengerMutex,
			EventQueue<TBuffer> recvQueue, 
			FatalExceptionCallback exCallback) {		
		_header = Objects.requireNonNull(header);
		_messengerMutex = Objects.requireNonNull(messengerMutex);
		_recvQueuePublisher = recvQueue.createPublisher(name, true);
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {	
		try {
			try {	
				_messengerMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

					@Override
					public void asOwner(Messenger<TBuffer> messenger) {
						TBuffer incomingBuffer;
						while (!Thread.interrupted()) {
							incomingBuffer = _recvQueuePublisher.next();
							if (messenger.recv(incomingBuffer, _header, true)) {
								_recvQueuePublisher.publish();
							}
						}
					}
					
				});
			} catch (MessengerClosedException eClosed) {
				return;
			} 
		} catch (Throwable t) {
			_exCallback.signalFatalException(t);
		} finally {
			if (_recvQueuePublisher.hasUnpublished()) {
				ResizingBuffer unpublished = _recvQueuePublisher.getUnpublished();
				_header.setIsValid(unpublished, false);
				_recvQueuePublisher.publish();
			}
		}
	}
	
}
