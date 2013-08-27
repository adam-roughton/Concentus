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

public class PubSubPattern {

	private static final int SUB_ID_SEGMENT_INDEX = 0;
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> SendTask<TSendHeader> asTask(
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(ResizingBuffer outgoingBuffer, TSendHeader header) {
				EventPattern.validate(header, 2);
				writePubEvent(outgoingBuffer, header, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> SendTask<TSendHeader> asTask(
			final int subId,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(ResizingBuffer outgoingBuffer, TSendHeader header) {
				EventPattern.validate(header, 2);
				writePubEvent(outgoingBuffer, header, subId, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> void writePubEvent(
			ResizingBuffer outgoingBuffer,
			TSendHeader header,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter) {
		writePubEvent(outgoingBuffer, header, eventHelper.getTypeId(), eventHelper, eventWriter);
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> void writePubEvent(
			ResizingBuffer outgoingBuffer,
			TSendHeader header,
			int subId,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter) {
		int cursor = header.getEventOffset();
		outgoingBuffer.writeInt(cursor, subId);
		header.setSegmentMetaData(outgoingBuffer, SUB_ID_SEGMENT_INDEX, cursor, 4);
		cursor += 4;
		EventPattern.writeContent(outgoingBuffer, cursor, header, eventHelper, eventWriter);
	}
	
	public static int readSubId(final ResizingBuffer incomingBuffer, final IncomingEventHeader header) {
		int subIdMetaData = header.getSegmentMetaData(incomingBuffer, SUB_ID_SEGMENT_INDEX);
		int subIdOffset = EventHeader.getSegmentOffset(subIdMetaData);
		return incomingBuffer.readInt(subIdOffset);
	}
	
}
