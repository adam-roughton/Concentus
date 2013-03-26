package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventHeader;
import com.adamroughton.consentus.messaging.IncomingEventHeader;
import com.adamroughton.consentus.messaging.OutgoingEventHeader;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public class RouterPattern {
	
	private static final int SOCKET_ID_SEGMENT_INDEX = 0;
	
	public static <TEvent extends ByteArrayBackedEvent> SendTask asTask(
			final byte[] socketId,
			final TEvent eventHelper, 
			final EventWriter<TEvent> eventWriter) {
		return new SendTask() {

			@Override
			public void write(byte[] outgoingBuffer, OutgoingEventHeader header) {
				EventPattern.validate(header, 2);
				writeEvent(outgoingBuffer, header, socketId, eventHelper, eventWriter);
			}
			
		};
	}
	
	public static <TEvent extends ByteArrayBackedEvent> void writeEvent(
			final byte[] outgoingBuffer,
			final OutgoingEventHeader header,
			final byte[] socketId,
			final TEvent eventHelper, 
			final EventWriter<TEvent> eventWriter) {
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
		doCopySocketId(incomingBuffer, senderId, socketIdSegmentMetaData, socketIdLength);
		return senderId;
	}
	
	public static void copySocketId(byte[] incomingBuffer, final IncomingEventHeader header, byte[] dest, int offset, int length) {
		int socketIdSegmentMetaData = header.getSegmentMetaData(incomingBuffer, SOCKET_ID_SEGMENT_INDEX);
		doCopySocketId(incomingBuffer, dest, socketIdSegmentMetaData, length);
	}
	
	private static void doCopySocketId(byte[] incomingBuffer, byte[] dest, int socketIdSegmentMetaData, int maxLengthToCopy) {
		int socketIdOffset = EventHeader.getSegmentOffset(socketIdSegmentMetaData);
		int socketIdLength = EventHeader.getSegmentLength(socketIdSegmentMetaData);
		System.arraycopy(incomingBuffer, socketIdOffset, dest, socketIdOffset, maxLengthToCopy > socketIdLength? socketIdLength : maxLengthToCopy);
	}
	
}
