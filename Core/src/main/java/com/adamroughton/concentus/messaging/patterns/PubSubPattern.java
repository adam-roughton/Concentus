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

import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;

public class PubSubPattern {

	private static final int SUB_ID_SEGMENT_INDEX = 0;
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> SendTask<TSendHeader> asTask(
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(byte[] outgoingBuffer, TSendHeader header) {
				EventPattern.validate(header, 2);
				writePubEvent(outgoingBuffer, header, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> SendTask<TSendHeader> asTask(
			final int subId,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(byte[] outgoingBuffer, TSendHeader header) {
				EventPattern.validate(header, 2);
				writePubEvent(outgoingBuffer, header, subId, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> void writePubEvent(
			final byte[] outgoingBuffer,
			final TSendHeader header,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		writePubEvent(outgoingBuffer, header, eventHelper.getEventTypeId(), eventHelper, eventWriter);
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> void writePubEvent(
			final byte[] outgoingBuffer,
			final TSendHeader header,
			final int subId,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		int cursor = header.getEventOffset();
		MessageBytesUtil.writeInt(outgoingBuffer, cursor, subId);
		header.setSegmentMetaData(outgoingBuffer, SUB_ID_SEGMENT_INDEX, cursor, 4);
		cursor += 4;
		EventPattern.writeContent(outgoingBuffer, cursor, header, eventHelper, eventWriter);
	}
	
	public static int readSubId(final byte[] incomingBuffer, final IncomingEventHeader header) {
		int subIdMetaData = header.getSegmentMetaData(incomingBuffer, SUB_ID_SEGMENT_INDEX);
		int subIdOffset = EventHeader.getSegmentOffset(subIdMetaData);
		return MessageBytesUtil.readInt(incomingBuffer, subIdOffset);
	}
	
}
