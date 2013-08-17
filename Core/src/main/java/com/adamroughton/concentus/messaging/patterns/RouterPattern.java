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
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.events.BufferBackedObject;

public class RouterPattern {
	
	private static final int SOCKET_ID_SEGMENT_INDEX = 0;
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> SendTask<TSendHeader> asUnreliableTask(
			byte[] socketId,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter) {
		return asTask(socketId, false, eventHelper, eventWriter);
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> SendTask<TSendHeader> asReliableTask(
			byte[] socketId,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter) {
		return asTask(socketId, true, eventHelper, eventWriter);
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> SendTask<TSendHeader> asTask(
			final byte[] socketId,
			final boolean isReliable,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(ResizingBuffer outgoingBuffer, TSendHeader header) {
				EventPattern.validate(header, 2);
				writeEvent(outgoingBuffer, header, socketId, isReliable, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> void writeUnreliableEvent(
			ResizingBuffer outgoingBuffer,
			TSendHeader header,
			byte[] socketId,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter)  {
		writeEvent(outgoingBuffer, header, socketId, false, eventHelper, eventWriter);
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> void writeReliableEvent(
			ResizingBuffer outgoingBuffer,
			TSendHeader header,
			byte[] socketId,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter)  {
		writeEvent(outgoingBuffer, header, socketId, true, eventHelper, eventWriter);
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends BufferBackedObject> void writeEvent(
			ResizingBuffer outgoingBuffer,
			TSendHeader header,
			byte[] socketId,
			boolean isReliable,
			TEvent eventHelper, 
			EventWriter<TSendHeader, TEvent> eventWriter) {
		int cursor = header.getEventOffset();
		outgoingBuffer.copyFrom(socketId, 0, cursor, socketId.length);
		header.setSegmentMetaData(outgoingBuffer, SOCKET_ID_SEGMENT_INDEX, cursor, socketId.length);
		header.setIsReliable(outgoingBuffer, isReliable);
		cursor += socketId.length;
		EventPattern.writeContent(outgoingBuffer, cursor, header, eventHelper, eventWriter);
	}
	
	public static byte[] getSocketId(ResizingBuffer incomingBuffer, final IncomingEventHeader header) {
		int socketIdSegmentMetaData = header.getSegmentMetaData(incomingBuffer, SOCKET_ID_SEGMENT_INDEX);
		int socketIdLength = EventHeader.getSegmentLength(socketIdSegmentMetaData);
		byte[] senderId = new byte[socketIdLength];
		doCopySocketId(incomingBuffer, socketIdSegmentMetaData, senderId, 0, socketIdLength);
		return senderId;
	}
	
	public static void copySocketId(ResizingBuffer incomingBuffer, final IncomingEventHeader header, byte[] dest, int offset, int length) {
		int socketIdSegmentMetaData = header.getSegmentMetaData(incomingBuffer, SOCKET_ID_SEGMENT_INDEX);
		doCopySocketId(incomingBuffer, socketIdSegmentMetaData, dest, offset, length);
	}
	
	private static void doCopySocketId(ResizingBuffer incomingBuffer, int socketIdSegmentMetaData, byte[] dest, int destOffset, int maxLengthToCopy) {
		int socketIdOffset = EventHeader.getSegmentOffset(socketIdSegmentMetaData);
		int socketIdLength = EventHeader.getSegmentLength(socketIdSegmentMetaData);
		incomingBuffer.copyTo(dest, socketIdOffset, destOffset, Math.min(maxLengthToCopy, socketIdLength));
	}
	
}
