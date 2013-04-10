package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventHeader;
import com.adamroughton.consentus.messaging.IncomingEventHeader;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.OutgoingEventHeader;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

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
