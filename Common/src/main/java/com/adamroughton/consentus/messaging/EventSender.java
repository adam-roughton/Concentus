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

	private final static int DEFAULT_MSG_PART_BUF_SIZE = 32;
	
	private final EventProcessingHeader _header;
	private final int _msgOffset;
	private byte[] _msgPartBytesBuffer = new byte[DEFAULT_MSG_PART_BUF_SIZE];
	
	private final int _baseZmqFlag;
	
	public EventSender(final EventProcessingHeader processingHeader,
			final boolean isNoBlock) {
		_header = Objects.requireNonNull(processingHeader);
		_msgOffset = _header.getEventOffset();
		
		if (isNoBlock) {
			_baseZmqFlag = ZMQ.NOBLOCK;
		} else {
			_baseZmqFlag = 0;
		}
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if the socket is ready.
	 * @param socket the socket to send the event on
	 * @param outgoingBuffer the buffer to send from
	 * @param msgPartOffsets the policy used to construct message parts from the
	 * ring buffer entry
	 * @return whether an event was sent.
	 */
	public boolean send(final ZMQ.Socket socket, 
			byte[] outgoingBuffer,
			final MessagePartBufferPolicy msgPartOffsets) {	
		if (msgPartOffsets.getMinReqBufferSize() > outgoingBuffer.length - _msgOffset) {
			throw new IllegalArgumentException(String.format(
					"The message part buffer policy requires a buffer size (%d) greater than" +
					" the underlying buffer of this sender (%d - note %d reserved for flags).", 
					msgPartOffsets.getMinReqBufferSize(),
					outgoingBuffer.length - _msgOffset,
					_msgOffset));
		}
		
		// only send if the event is valid
		if (!_header.isValid(outgoingBuffer)) return false;
		
		int partCount = msgPartOffsets.partCount();
		int offset;
		byte[] msgPart;
		int zmqFlag;
		boolean success = true;
		for (int i = 0; i < partCount; i++) {
			// for every msg part that is not the last one
			if (i < partCount - 1) {
				int reqLength = msgPartOffsets.getOffset(i + 1) - msgPartOffsets.getOffset(i);
				msgPart = getBuffer(reqLength);
				offset = msgPart.length - reqLength;
				System.arraycopy(outgoingBuffer, msgPartOffsets.getOffset(i) + _msgOffset, msgPart, offset, reqLength);
				zmqFlag = ZMQ.SNDMORE | _baseZmqFlag;
			} else {
				offset = msgPartOffsets.getOffset(i) + _msgOffset;
				msgPart = outgoingBuffer;
				zmqFlag = _baseZmqFlag;
			}
			success = socket.send(msgPart, offset, zmqFlag);
			if (!success)
				return false;
		}
		return success;
	}
	
	private byte[] getBuffer(int reqSpace) {
		if (_msgPartBytesBuffer.length < reqSpace) {
			_msgPartBytesBuffer = new byte[reqSpace];
		}
		return _msgPartBytesBuffer;
	}
	
}
