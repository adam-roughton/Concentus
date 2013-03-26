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

public class EventSender {

	private final OutgoingEventHeader _header;
	private final int _baseZmqFlag;
	
	public EventSender(final OutgoingEventHeader header,
			final boolean isNoBlock) {
		_header = Objects.requireNonNull(header);
		
		if (isNoBlock) {
			_baseZmqFlag = ZMQ.NOBLOCK;
		} else {
			_baseZmqFlag = 0;
		}
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if the socket is ready.
	 * @param socketPackage the socket (plus additional settings) to send the event on
	 * @param outgoingBuffer the buffer to send from
	 * @return whether an event was sent.
	 */
	public boolean send(final SocketPackage socketPackage, 
			byte[] outgoingBuffer) {
		return send(socketPackage.getSocket(),
				outgoingBuffer);
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if the socket is ready.
	 * 
	 * @param socket the socket to send the event on
	 * @param outgoingBuffer the buffer to send from
	 * @return {@code true} if the complete event was sent, {@code false} otherwise
	 */
	public boolean send(final ZMQ.Socket socket,
			byte[] outgoingBuffer) {
		// only send if the event is valid
		if (!_header.isValid(outgoingBuffer)) return false;	
		
		// check event bounds
		int segmentCount = _header.getSegmentCount();
		int lastSegmentMetaData = _header.getSegmentMetaData(outgoingBuffer, segmentCount - 1);
		int lastSegmentOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		int lastSegmentLength = EventHeader.getSegmentLength(lastSegmentMetaData);
		int requiredLength = lastSegmentOffset + lastSegmentLength;
		if (requiredLength > outgoingBuffer.length)
			throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
					outgoingBuffer.length, requiredLength));
		
		int segmentIndex;
		if (_header.isPartiallySent(outgoingBuffer)) {
			segmentIndex = _header.getNextSegmentToSend(outgoingBuffer);
		} else {
			segmentIndex = 0;
		}
		for (;segmentIndex < segmentCount; segmentIndex++) {
			int segmentMetaData = _header.getSegmentMetaData(outgoingBuffer, segmentIndex);
			int offset = EventHeader.getSegmentOffset(segmentMetaData);
			int length = EventHeader.getSegmentLength(segmentMetaData);
			int flags = _baseZmqFlag | ((segmentIndex < segmentCount - 1)? ZMQ.SNDMORE : 0);
			if (!socket.send(outgoingBuffer, offset, length, flags)) {
				_header.setNextSegmentToSend(outgoingBuffer, segmentIndex);
				_header.setIsPartiallySent(outgoingBuffer, true);
				return false;
			}
		}
		return true;
	}
	
}
