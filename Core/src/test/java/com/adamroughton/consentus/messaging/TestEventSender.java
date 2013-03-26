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
package com.adamroughton.consentus.messaging;

import java.util.List;
import java.util.Objects;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;

@RunWith(MockitoJUnitRunner.class)
public class TestEventSender {

	private static final int BUFFER_SIZE = 512;
	
	@Mock private ZMQ.Socket _zmqSocket;
	private SocketPackage _socketPackage;
	private byte[] _outgoingBuffer;
	
	@Before
	public void setUp() {
		_outgoingBuffer = new byte[BUFFER_SIZE];
		
		_socketPackage = SocketPackage.create(_zmqSocket)
				.setSocketId(0);
	}
	
	@Test
	public void sendSingleSegment() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 1);
		EventSender sender = new EventSender(header, false);
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		int segmentLength = 256;
		
		byte[] content = new byte[segmentLength];
		for (int i = 0; i < segmentLength; i += 4) {
			MessageBytesUtil.writeInt(content, i, i / 4);
		}
		
		int[] expectedOffsets = writeMessage(new byte[][] {content}, _outgoingBuffer, header);		
		assertTrue(sender.send(_socketPackage, _outgoingBuffer));
		
		ArgumentCaptor<byte[]> eBytesCaptor = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		verify(_zmqSocket).send(eBytesCaptor.capture(), eOffsetCaptor.capture(), eLengthCaptor.capture(), intThat(doesNotHaveFlags(ZMQ.SNDMORE)));
		
		List<byte[]> eventParts = eBytesCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
		
		// contents
		byte[] messageBytes = eventParts.get(0);
		int actualOffset = offsets.get(0);
		assertEquals(expectedOffsets[0], actualOffset);
		int actualLength = (int) lengths.get(0);
		assertEquals(segmentLength, actualLength);
		assertRangeEqual(content, messageBytes, actualOffset, actualLength);	
	}
	
	@Test
	public void sendMultipleSegments() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 3);
		EventSender sender = new EventSender(header, false);
		
		ByteArrayCaptor eventPartCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		when(_zmqSocket.send(argThat(eventPartCaptor), eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		byte[] content1 = new byte[32];
		byte[] content2 = new byte[32];
		byte[] content3 = new byte[32];
		byte[][] contents = new byte[][] {content1, content2, content3};
		
		for (int i = 0; i < contents.length; i++) {
			for (int j = 0; j < contents[i].length; j += 4) {
				MessageBytesUtil.writeInt(contents[i], j, -(i + 1));
			}
		}
		
		int[] expectedOffsets = writeMessage(contents, _outgoingBuffer, header);
		assertTrue(sender.send(_socketPackage, _outgoingBuffer));
		
		verify(_zmqSocket, times(2)).send(any(byte[].class), anyInt(), anyInt(), intThat(hasFlags(ZMQ.SNDMORE)));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), anyInt(), intThat(doesNotHaveFlags(ZMQ.SNDMORE)));
		
		List<byte[]> eventParts = eventPartCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
		assertEquals(3, eventParts.size());
		for (int i = 0; i < 3; i++) {
			int actualLength = (int)lengths.get(i);
			assertEquals(contents[i].length, actualLength);
			int actualOffset = (int)offsets.get(i);
			assertEquals(expectedOffsets[i], actualOffset);
			byte[] contentBytes = eventParts.get(i);
			assertRangeEqual(contents[i], contentBytes, actualOffset, actualLength);
		}
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void sendSegmentLengthZero() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 3);
		EventSender sender = new EventSender(header, false);
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		// write the segments out
		writeMessage(new byte[][] { new byte[0], new byte[0], new byte[0] }, _outgoingBuffer, header);
		
		assertTrue(sender.send(_socketPackage, _outgoingBuffer));
	}
	
	@Test(expected=RuntimeException.class)
	public void sendSegmentLengthTooLargeForBuffer() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 3);
		EventSender sender = new EventSender(header, false);
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
	
		byte[] content1 = new byte[32];
		byte[] content2 = new byte[32];
		byte[] content3 = new byte[32];
		byte[][] contents = new byte[][] {content1, content2, content3};
		
		for (int i = 0; i < contents.length; i++) {
			for (int j = 0; j < contents[i].length; j += 4) {
				MessageBytesUtil.writeInt(contents[i], j, -(i + 1));
			}
		}
		
		writeMessage(contents, _outgoingBuffer, header);
		int lastSegmentMetaData = header.getSegmentMetaData(_outgoingBuffer, 2);
		int lastOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		header.setSegmentMetaData(_outgoingBuffer, 2, lastOffset, BUFFER_SIZE);
		sender.send(_socketPackage, _outgoingBuffer);
	}
	
	/**
	 * Writes the given segments into the provided buffer with the correct protocol
	 * format.
	 * @param segments the segments to write
	 * @param outgoingBuffer the buffer to write into
	 * @param header the processing header to use for writing
	 * @return the segment offsets in the outgoing buffer (saves having to use header to extract this data)
	 */
	public static int[] writeMessage(byte[][] segments, byte[] outgoingBuffer, OutgoingEventHeader header) {
		int[] offsets = new int[segments.length];
		Objects.requireNonNull(outgoingBuffer);
		
		if (segments.length > header.getSegmentCount())
			fail(String.format("Too many segments for the given header (%d > %d)", segments.length, header.getSegmentCount()));
		else if (segments.length < header.getSegmentCount())
			fail(String.format("Not enough segments for the given header (%d < %d)", segments.length, header.getSegmentCount()));
		
		int contentCursor = header.getEventOffset();
		for (int segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
			byte[] segment = segments[segmentIndex];
			if (segment.length > outgoingBuffer.length - contentCursor)
				fail(String.format("Test failure: allocated buffer too small for segment length " +
						"(segIndex = %d, remainingBuffer = %d, bufferSize = %d)", 
						segmentIndex, 
						outgoingBuffer.length - contentCursor, 
						outgoingBuffer.length));
			header.setSegmentMetaData(outgoingBuffer, segmentIndex, contentCursor, segment.length);
			offsets[segmentIndex] = contentCursor;
			System.arraycopy(segment, 0, outgoingBuffer, contentCursor, segment.length);
			contentCursor += segment.length;
		}
		header.setIsValid(outgoingBuffer, true);
		return offsets;
	}
	
}
