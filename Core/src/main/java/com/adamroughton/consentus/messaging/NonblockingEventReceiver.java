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

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * Handles receiving events from passed sockets into the attached
 * {@link RingBuffer} without blocking.
 * 
 * @author Adam Roughton
 *
 */
public class NonblockingEventReceiver {	
	
	private final EventReceiver _eventReceiver;
	private final RingBuffer<byte[]> _incomingBuffer;
	private final Sequence _sequence;
	private final EventProcessingHeader _header;
	
	/**
	 * As we might not recv a message during a {@link recvIfReady} call,
	 * but require a claimed sequence to recv (to maximise memory spatial
	 * locality), we track whether claimed sequences on the incoming buffer
	 * have been used (otherwise we claim another).
	 */
	private long _unpubClaimedSeq = -1;
	
	public NonblockingEventReceiver(final RingBuffer<byte[]> incomingBuffer, 
			final EventProcessingHeader processingHeader) {
		_eventReceiver = new EventReceiver(processingHeader, true);
		_incomingBuffer = Objects.requireNonNull(incomingBuffer);
		_sequence = new Sequence(-1);
		_header = Objects.requireNonNull(processingHeader);
	}
	
	/**
	 * Attempts to receive an event if an event is immediately available, and there is
	 * space in the ring buffer for it.
	 * @param socketPackage the socket to receive on
	 * @return whether an event was placed in the buffer. This will return true even
	 * if the event is corrupt or had an unexpected number of event parts
	 * @see equivalent to NonblockingEventReceiver#recvIfReady(org.zeromq.ZMQ.Socket, MessagePartBufferPolicy, int)
	 */
	public boolean recvIfReady(final SocketPackage socketPackage) {
		return recvIfReady(socketPackage.getSocket(), 
				socketPackage.getMessageFrameBufferMapping(), 
				socketPackage.getSocketId());
	}
	
	/**
	 * Attempts to receive an event if an event is immediately available, and there is
	 * space in the ring buffer for it.
	 * @param socket the socket to receive on
	 * @param mapping the mapping to apply to received messages
	 * @param socketId the ID to put in the header of received messages
	 * @return whether an event was placed in the buffer. This will return true even
	 * if the event is corrupt or had an unexpected number of event parts
	 */
	public boolean recvIfReady(final ZMQ.Socket socket,
			final MessageFrameBufferMapping mapping,
			final int socketId) {
		if (_unpubClaimedSeq == -1 && _incomingBuffer.hasAvailableCapacity(1)) {
			_unpubClaimedSeq = _incomingBuffer.next();					
		}	
		// only recv if slots are available
		if (_unpubClaimedSeq != -1) {
			byte[] incomingBuffer = _incomingBuffer.get(_unpubClaimedSeq);
			if (_eventReceiver.recv(socket, mapping, socketId, incomingBuffer)) {
				publish();
				return true;
			}
		}
		return false;
	}
	
	private void publish() {
		_incomingBuffer.publish(_unpubClaimedSeq);
		_sequence.set(_unpubClaimedSeq);
		_unpubClaimedSeq = -1;
	}
	
	public void tidyUp() {
		if (_unpubClaimedSeq != -1) {
			byte[] incomingBuffer = _incomingBuffer.get(_unpubClaimedSeq);
			_header.setIsValid(false, incomingBuffer);
			publish();
		}
	}
	
	public Sequence getSequence() {
		return _sequence;
	}
}