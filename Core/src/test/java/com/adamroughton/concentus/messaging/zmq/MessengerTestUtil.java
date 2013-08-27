package com.adamroughton.concentus.messaging.zmq;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Objects;
import java.util.UUID;

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public final class MessengerTestUtil {

	public static byte[] genContent(int length) {
		return genContent(length, 0);
	}
	
	public static byte[] genContent(int length, int seed) {
		byte[] content = new byte[length];
		for (int i = 0; i < length; i += 4) {
			BytesUtil.writeInt(content, i, i / 4 + seed);
		}
		return content;
	}
	
	public static byte[] genIdBytes(UUID id) {
		byte[] idBytes = new byte[16];
		BytesUtil.writeUUID(idBytes, 0, id);
		return idBytes;
	}
	
	public static void genSegmentData(int length, int seed, byte[][] segments, int...indices) {
		for (int index : indices) {
			if (index < 0 || index >= segments.length) 
				throw new IllegalArgumentException(String.format("Provided index '%d' out of bounds [%d, %d]", index, 0, segments.length));
			segments[index] = genContent(length, seed++);
		}
	}
	
	public static byte[] makeMsgHeader(byte[] msg) {
		return makeMsgHeader(msg.length);
	}
	
	public static byte[] makeMsgHeader(byte[]... msgs) {
		int total = 0;
		for (byte[] msg : msgs) {
			total += msg.length;
		}
		return makeMsgHeader(total);
	}
	
	public static byte[] makeMsgHeader(int msgLength) {
		byte[] header = new byte[ResizingBuffer.INT_SIZE];
		BytesUtil.writeInt(header, 0, msgLength);
		return header;
	}
	
	public static byte[] makeMsgHeader(long seq, byte[]... msgs) {
		int total = 0;
		for (byte[] msg : msgs) {
			total += msg.length;
		}
		return makeMsgHeader(seq, total);
	}
	
	public static byte[] makeMsgHeader(long seq, byte[] msg) {
		return makeMsgHeader(seq, msg.length);
	}
	
	public static byte[] makeMsgHeader(long seq, int msgLength) {
		byte[] header = new byte[ResizingBuffer.LONG_SIZE + ResizingBuffer.INT_SIZE];
		BytesUtil.writeLong(header, 0, seq);
		BytesUtil.writeInt(header, ResizingBuffer.LONG_SIZE, msgLength);
		return header;
	}
	
	public static byte[][] readMessageParts(ArrayBackedResizingBuffer buffer, IncomingEventHeader header) {
		Objects.requireNonNull(buffer);
		assertTrue(header.isValid(buffer));
		
		byte[][] segments = new byte[header.getSegmentCount()][];
		
		for (int segmentIndex = 0; segmentIndex < header.getSegmentCount(); segmentIndex++) {
			int segmentMetaData = header.getSegmentMetaData(buffer, segmentIndex);
			int segmentOffset = EventHeader.getSegmentOffset(segmentMetaData);
			int segmentLength = EventHeader.getSegmentLength(segmentMetaData);
			
			byte[] segment = new byte[segmentLength];
			buffer.copyTo(segment, 0, segmentOffset, segmentLength);
			segments[segmentIndex] = segment;
		}
		
		return segments;
	}
	
	/**
	 * Writes the given segments into the provided buffer with the correct protocol
	 * format.
	 * @param segments the segments to write
	 * @param outgoingBuffer the buffer to write into
	 * @param header the processing header to use for writing
	 * @return the segment offsets in the outgoing buffer (saves having to use header to extract this data)
	 */
	public static int[] writeMessage(byte[][] segments, ArrayBackedResizingBuffer outgoingBuffer, OutgoingEventHeader header) {
		int[] offsets = new int[segments.length];
		Objects.requireNonNull(outgoingBuffer);
		
		if (segments.length > header.getSegmentCount())
			fail(String.format("Too many segments for the given header (%d > %d)", segments.length, header.getSegmentCount()));
		else if (segments.length < header.getSegmentCount())
			fail(String.format("Not enough segments for the given header (%d < %d)", segments.length, header.getSegmentCount()));
		
		int contentCursor = header.getEventOffset();
		for (int segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
			byte[] segment = segments[segmentIndex];
			header.setSegmentMetaData(outgoingBuffer, segmentIndex, contentCursor, segment.length);
			offsets[segmentIndex] = contentCursor;
			outgoingBuffer.copyFrom(segment, 0, contentCursor, segment.length);
			contentCursor += segment.length;
		}
		header.setIsValid(outgoingBuffer, true);
		return offsets;
	}
	
}
