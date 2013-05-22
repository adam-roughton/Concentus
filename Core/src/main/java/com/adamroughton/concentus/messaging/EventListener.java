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
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.lmax.disruptor.RingBuffer;

public final class EventListener implements Runnable {
	
	private final IncomingEventHeader _header;
	private final RingBuffer<byte[]> _ringBuffer;
	private final FatalExceptionCallback _exCallback;
	private final Mutex<Messenger> _messengerMutex;
	
	public EventListener(
			IncomingEventHeader header,
			Mutex<Messenger> messengerMutex,
			RingBuffer<byte[]> ringBuffer, 
			FatalExceptionCallback exCallback) {		
		_header = Objects.requireNonNull(header);
		_messengerMutex = Objects.requireNonNull(messengerMutex);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_exCallback = Objects.requireNonNull(exCallback);
	}

	@Override
	public void run() {	
		try {
			try {	
				_messengerMutex.runAsOwner(new OwnerDelegate<Messenger>() {

					@Override
					public void asOwner(Messenger messenger) {
						while (!Thread.interrupted()) {
							nextEvent(messenger);
						}
					}
					
				});
			} catch (MessengerClosedException eClosed) {
				return;
			} 
		} catch (Throwable t) {
			_exCallback.signalFatalException(t);
		}
	}
	
	private void nextEvent(Messenger messenger) {
		final long seq = _ringBuffer.next();
		final byte[] incomingBuffer = _ringBuffer.get(seq);
		try {
			if (!messenger.recv(incomingBuffer, _header, true)) {
				_header.setIsValid(incomingBuffer, false);
			}
		} finally {
			_ringBuffer.publish(seq);
		}
	}
	
}
