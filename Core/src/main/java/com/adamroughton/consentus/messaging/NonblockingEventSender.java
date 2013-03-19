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

import org.zeromq.ZMQ;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

/**
 * Handles sending events from the attached ring buffer,
 * not waiting if no events are available to send or the socket
 * is not ready.
 * 
 * @author Adam Roughton
 *
 */
public class NonblockingEventSender {
	
	private final EventSender _eventSender;
	private final RingBuffer<byte[]> _outgoingBuffer;
	private final SequenceBarrier _outgoingBarrier;
	private final Sequence _sequence;
	private long _availableSeq = -1;
	
	public NonblockingEventSender(final RingBuffer<byte[]> outgoingBuffer,
			final SequenceBarrier outgoingBarrier, 
			final EventProcessingHeader processingHeader) {
		_eventSender = new EventSender(processingHeader, true);
		_sequence = new Sequence(-1);
		_outgoingBuffer = outgoingBuffer;
		_outgoingBarrier = outgoingBarrier;
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if both the event is ready and the socket is able to send the event.
	 * @param socketPackage the socket (plus additional settings) to send the event on
	 * @return whether an event was sent from the buffer.
	 * @see equivalent to NonblockingEventSender#sendIfReady(org.zeromq.ZMQ.Socket, MessagePartBufferPolicy)
	 */
	public boolean sendIfReady(final SocketPackage socketPackage) {
		return sendIfReady(socketPackage.getSocket(), 
				socketPackage.getMessagePartPolicy());
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if both the event is ready and the socket is able to send the event.
	 * @param socket the socket to send the event on
	 * @param msgPartPolicy the message part policy to apply when sending messages
	 * @return whether an event was sent from the buffer.
	 */
	public boolean sendIfReady(final ZMQ.Socket socket,
			final MessagePartBufferPolicy msgPartPolicy) {
		long nextSeq = _sequence.get() + 1;
		if (nextSeq > _availableSeq) {
			_availableSeq = _outgoingBarrier.getCursor();
		}
		if (nextSeq <= _availableSeq) {
			byte[] outgoingBuffer = _outgoingBuffer.get(nextSeq);
			if (_eventSender.send(socket, msgPartPolicy, outgoingBuffer)) {
				_sequence.addAndGet(1);
				return true;
			}
		}
		return false;
	}
	
	public Sequence getSequence() {
		return _sequence;
	}
}