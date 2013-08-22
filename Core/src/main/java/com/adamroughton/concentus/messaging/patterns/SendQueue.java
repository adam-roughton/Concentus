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
package com.adamroughton.concentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;

public class SendQueue<TSendHeader extends OutgoingEventHeader, TBuffer extends ResizingBuffer> {

	private final TSendHeader _header;
	private final EventQueuePublisher<TBuffer> _sendQueuePublisher;
	
	public SendQueue(
			String name,
			TSendHeader header, 
			EventQueue<TBuffer> sendQueue) {
		_header = Objects.requireNonNull(header);
		_sendQueuePublisher = sendQueue.createPublisher(name, false);
	}
	
	public final void send(SendTask<TSendHeader> task) {
		while (!trySend(task));
	}
	
	public final void send(ResizingBuffer src, IncomingEventHeader headerOrigin) {
		send(src, 0, src.getContentSize(), headerOrigin);
	}
	
	public final void send(ResizingBuffer src, int offset, int length, IncomingEventHeader headerOrigin) {
		while (!trySend(src, offset, length, headerOrigin));
	}
	
	/**
	 * Sends the task if there is space, failing if the call
	 * would block.
	 * @param task the task to send
	 * @return {@code true} if the task was send without blocking,
	 * {@code false} otherwise
	 */
	public final boolean trySend(SendTask<TSendHeader> task) {
		ResizingBuffer outgoingBuffer = _sendQueuePublisher.next();
		if (outgoingBuffer == null) return false;
		
		boolean wasSuccessful;
		try {
			_header.reset(outgoingBuffer);
			task.write(outgoingBuffer, _header);
		} finally {
			wasSuccessful = _sendQueuePublisher.publish();
		}
		return wasSuccessful;
	}
	
	public final boolean trySend(ResizingBuffer src, IncomingEventHeader headerOrigin) {
		return trySend(src, 0, src.getContentSize(), headerOrigin);
	}
	
	public final boolean trySend(ResizingBuffer src, int offset, int length, IncomingEventHeader headerOrigin) {
		ResizingBuffer outgoingBuffer = _sendQueuePublisher.next();
		if (outgoingBuffer == null) return false;
		
		boolean wasSuccessful;
		try {
			_header.reset(outgoingBuffer);
			
			if (headerOrigin.getSegmentCount() != _header.getSegmentCount())
				throw new IllegalArgumentException(String.format("The send header and origin header must " +
						"have matching segment counts (expected %d, was %d)", 
						_header.getSegmentCount(), headerOrigin.getSegmentCount()));
			_header.setIsValid(outgoingBuffer, headerOrigin.isValid(src));
			_header.setIsMessagingEvent(outgoingBuffer, headerOrigin.isMessagingEvent(src));
			
			int cursor = _header.getEventOffset();
			for (int i = 0; i < _header.getSegmentCount(); i++) {
				int segmentMetaData = headerOrigin.getSegmentMetaData(src, i);
				int segmentOffset = EventHeader.getSegmentOffset(segmentMetaData);
				int segmentLength = EventHeader.getSegmentLength(segmentMetaData);
				
				src.copyTo(outgoingBuffer, segmentOffset, cursor, segmentLength);
				_header.setSegmentMetaData(outgoingBuffer, i, cursor, segmentLength);
				
				cursor += segmentLength;
			}
		} finally {
			wasSuccessful = _sendQueuePublisher.publish();
		}
		return wasSuccessful;
	}
	
}
