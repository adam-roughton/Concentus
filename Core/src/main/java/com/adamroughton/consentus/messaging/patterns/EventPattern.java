package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventHeader;
import com.adamroughton.consentus.messaging.IncomingEventHeader;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.OutgoingEventHeader;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public class EventPattern {

	/**
	 * EventTask for events that only have content.
	 * @param eventHelper
	 * @param eventWriter
	 * @return
	 */
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> SendTask<TSendHeader> asTask(
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> eventWriter) {
		return new SendTask<TSendHeader>() {

			@Override
			public void write(byte[] outgoingBuffer, TSendHeader header) {
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
	public static <TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> int writeContent(
			final byte[] outgoingBuffer,
			final int offset,
			final TSendHeader header,
			final TEvent eventHelper, 
			final EventWriter<TSendHeader, TEvent> writer) {
		try {
			eventHelper.setBackingArray(outgoingBuffer, offset);
			try {
				writer.write(header, eventHelper);
				int length = eventHelper.getEventSize();
				int contentSegmentIndex = header.getSegmentCount() - 1;
				header.setSegmentMetaData(outgoingBuffer, contentSegmentIndex, offset, length);
				header.setIsValid(outgoingBuffer, true);
				return length;
			} catch (Exception e) {
				header.setIsValid(outgoingBuffer, false);
				throw new RuntimeException(e);
			}
		} finally {
			eventHelper.releaseBackingArray();
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
	public static <TRecvHeader extends IncomingEventHeader, TEvent extends ByteArrayBackedEvent> void readContent(
			final byte[] incomingBuffer, 
			final TRecvHeader header,
			final TEvent eventHelper,
			final EventReader<TRecvHeader, TEvent> reader) {
		int contentOffset = getContentOffset(incomingBuffer, header);
		eventHelper.setBackingArray(incomingBuffer, contentOffset);
		try {
			reader.read(header, eventHelper);
		} finally {
			eventHelper.releaseBackingArray();
		}
	}
	
	public static EventHeader validate(EventHeader header, int expectedSegmentCount) throws IllegalArgumentException {
		if (header.getSegmentCount() != expectedSegmentCount) {
			throw new IllegalArgumentException(String.format("This pattern only supports events with %d segments (header had %d).", 
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
	public static int getContentOffset(byte[] incomingBuffer, final IncomingEventHeader header) {
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
	public static int getEventType(byte[] incomingBuffer, IncomingEventHeader header) {
		return MessageBytesUtil.readInt(incomingBuffer, getContentOffset(incomingBuffer, header));
	}
	
}
