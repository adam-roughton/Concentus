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

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public class EventPattern {

	/**
	 * EventTask for events that only have content.
	 * @param eventHelper
	 * @param eventWriter
	 * @return
	 */
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> SendTask<TSendHeader> asTask(
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(ResizingBuffer outgoingBuffer, TSendHeader header) {
				validate(header, 1);
				writeContent(outgoingBuffer, header.getEventOffset(), header, eventHelper, eventWriter);
			}
			
		};
	}
	
	/**
	 * Writes the content bytes into the last message segment of the provided
	 * outgoingBuffer using the eventHelper and writer.
	 * @param outgoingBuffer the buffer to write into
	 * @param offset the offset where the content starts
	 * @param header the header associated with the outgoing buffer
	 * @param eventHelper the helper to use to translate the event bytes
	 * @param writer the writer to use to create the content
	 * @return the number of bytes written into the buffer
	 */
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> int writeContent(
			ResizingBuffer outgoingBuffer,
			int offset,
			TSendHeader header,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> writer) {
		try {
			eventHelper.attachToBuffer(outgoingBuffer, offset);
			try {
				eventHelper.writeTypeId();
				writer.write(header, eventHelper);
				int length = eventHelper.getBuffer().getContentSize();
				int contentSegmentIndex = header.getSegmentCount() - 1;
				header.setSegmentMetaData(outgoingBuffer, contentSegmentIndex, offset, length);
				header.setIsValid(outgoingBuffer, true);
				return length;
			} catch (Exception e) {
				header.setIsValid(outgoingBuffer, false);
				throw new RuntimeException(e);
			}
		} finally {
			eventHelper.releaseBuffer();
		}
	}
	
	/**
	 * Reads the last message segment of the incoming buffer using the
	 * provided eventHelper and reader.
	 * @param incomingBuffer the event bytes to process
	 * @param header the header associated with the incoming buffer
	 * @param eventHelper the helper to use to translate the event bytes
	 * @param reader the reader to use to extract the event data
	 */
	public static <TRecvHeader extends IncomingEventHeader, TEvent extends BufferBackedObject> void readContent(
			final ResizingBuffer incomingBuffer, 
			final TRecvHeader header,
			final TEvent eventHelper,
			final EventReader<TRecvHeader, TEvent> reader) {
		int contentOffset = getContentOffset(incomingBuffer, header);
		eventHelper.attachToBuffer(incomingBuffer, contentOffset);
		try {
			reader.read(header, eventHelper);
		} finally {
			eventHelper.releaseBuffer();
		}
	}
	
	public static EventHeader validate(EventHeader header, int expectedSegmentCount) throws IllegalArgumentException {
		if (header.getSegmentCount() < expectedSegmentCount) {
			throw new IllegalArgumentException(String.format("This pattern only supports events with at least %d segments (header had %d).", 
					expectedSegmentCount, header.getSegmentCount()));
		}
		return header;
	}
	
	/**
	 * Gets the offset of the content segment of the incoming buffer.
	 * @param incomingBuffer
	 * @param header
	 * @return
	 */
	public static int getContentOffset(ResizingBuffer incomingBuffer, final IncomingEventHeader header) {
		int contentSegmentIndex = header.getSegmentCount() - 1;
		int contentSegmentMetaData = header.getSegmentMetaData(incomingBuffer, contentSegmentIndex);
		return EventHeader.getSegmentOffset(contentSegmentMetaData);
	}
	
	/**
	 * Gets the event type of the event contained within the content segment of the incoming buffer (last segment).
	 * @param incomingBuffer
	 * @param header
	 * @return the event type ID of the event
	 */
	public static int getEventType(ResizingBuffer incomingBuffer, IncomingEventHeader header) {
		return incomingBuffer.readInt(getContentOffset(incomingBuffer, header));
	}
	
}
