package com.adamroughton.concentus.messaging.patterns;

import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;

public class RouterPattern {
	
	private static final int SOCKET_ID_SEGMENT_INDEX = 0;
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> SendTask<TSendHeader> asTask(
			final byte[] socketId,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(byte[] outgoingBuffer, TSendHeader header) {
				EventPattern.validate(header, 2);
				writeEvent(outgoingBuffer, header, socketId, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> void writeEvent(
			final byte[] outgoingBuffer,
			final TSendHeader header,
			final byte[] socketId,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		int cursor = header.getEventOffset();
		System.arraycopy(socketId, 0, outgoingBuffer, cursor, socketId.length);
		header.setSegmentMetaData(outgoingBuffer, SOCKET_ID_SEGMENT_INDEX, cursor, socketId.length);
		cursor += socketId.length;
		EventPattern.writeContent(outgoingBuffer, cursor, header, eventHelper, eventWriter);
	}
	
	public static byte[] getSocketId(final byte[] incomingBuffer, final IncomingEventHeader header) {
		int socketIdSegmentMetaData = header.getSegmentMetaData(incomingBuffer, SOCKET_ID_SEGMENT_INDEX);
		int socketIdLength = EventHeader.getSegmentLength(socketIdSegmentMetaData);
		byte[] senderId = new byte[socketIdLength];
		doCopySocketId(incomingBuffer, socketIdSegmentMetaData, senderId, 0, socketIdLength);
		return senderId;
	}
	
	public static void copySocketId(byte[] incomingBuffer, final IncomingEventHeader header, byte[] dest, int offset, int length) {
		int socketIdSegmentMetaData = header.getSegmentMetaData(incomingBuffer, SOCKET_ID_SEGMENT_INDEX);
		doCopySocketId(incomingBuffer, socketIdSegmentMetaData, dest, offset, length);
	}
	
	private static void doCopySocketId(byte[] incomingBuffer, int socketIdSegmentMetaData, byte[] dest, int destOffset, int maxLengthToCopy) {
		int socketIdOffset = EventHeader.getSegmentOffset(socketIdSegmentMetaData);
		int socketIdLength = EventHeader.getSegmentLength(socketIdSegmentMetaData);
		System.arraycopy(incomingBuffer, socketIdOffset, dest, destOffset, maxLengthToCopy > socketIdLength? socketIdLength : maxLengthToCopy);
	}
	
}
