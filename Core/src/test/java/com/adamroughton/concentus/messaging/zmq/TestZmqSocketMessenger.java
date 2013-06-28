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
package com.adamroughton.concentus.messaging.zmq;

import static com.adamroughton.concentus.messaging.ZmqTestUtil.assertRangeEqual;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.doesNotHaveFlags;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.fakeRecv;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.hasFlags;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.matchesLength;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.intThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.concentus.DrivableClock;
import com.adamroughton.concentus.messaging.ByteArrayCaptor;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

@RunWith(MockitoJUnitRunner.class)
public class TestZmqSocketMessenger {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final IncomingEventHeader _defaultHeader = new IncomingEventHeader(0, 2);
	
	@Mock private ZMQ.Socket _zmqSocket;
	private ZmqSocketMessenger _messenger;
	private byte[] _buffer;
	private DrivableClock _clock;
	
	@Before
	public void setUp() {
		_buffer = new byte[EVENT_BUFFER_LENGTH];
		_clock = new DrivableClock();
		_clock.setTime(5500000, TimeUnit.MILLISECONDS);
		_messenger = new ZmqSocketMessenger(0, "", _zmqSocket, _clock);
	}
	
	private byte[] genContent(int length) {
		return genContent(length, 0);
	}
	
	private byte[] genContent(int length, int seed) {
		byte[] content = new byte[length];
		for (int i = 0; i < length; i += 4) {
			MessageBytesUtil.writeInt(content, i, i / 4 + seed);
		}
		return content;
	}
	
	private byte[] genIdBytes(UUID id) {
		byte[] idBytes = new byte[16];
		MessageBytesUtil.writeUUID(idBytes, 0, id);
		return idBytes;
	}
	
	private byte[][] readMessageParts(byte[] bufferBytes, IncomingEventHeader header) {
		Objects.requireNonNull(bufferBytes);
		assertTrue(header.isValid(bufferBytes));
		
		byte[][] segments = new byte[header.getSegmentCount()][];
		
		for (int segmentIndex = 0; segmentIndex < header.getSegmentCount(); segmentIndex++) {
			int segmentMetaData = header.getSegmentMetaData(bufferBytes, segmentIndex);
			int segmentOffset = EventHeader.getSegmentOffset(segmentMetaData);
			int segmentLength = EventHeader.getSegmentLength(segmentMetaData);
			
			if (segmentOffset + segmentLength > bufferBytes.length) {
				fail(String.format("The segment length exceeded the remaining buffer size (%d > %d)", segmentLength, bufferBytes.length - segmentOffset));
			}
			byte[] segment = new byte[segmentLength];
			System.arraycopy(bufferBytes, segmentOffset, segment, 0, segmentLength);
			segments[segmentIndex] = segment;
		}
		
		return segments;
	}
	
	@Test
	public void recvSingleSegment() {
		IncomingEventHeader header = new IncomingEventHeader(0, 1);
		
		byte[] content = genContent(256);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(1, segments.length);
		assertArrayEquals(content, segments[0]);
	}
	
	@Test
	public void recvMultipleSegments() {
		IncomingEventHeader header = new IncomingEventHeader(0, 4);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[][] contentSegments = new byte[3][];
		for (int i = 0; i < 3; i++) {
			contentSegments[i] = genContent(32, i);
		}
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(contentSegments[0]))
			.then(fakeRecv(contentSegments[1]))
			.then(fakeRecv(contentSegments[2]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(4, segments.length);
		
		assertArrayEquals(expectedIdBytes, segments[0]);
		for (int i = 0; i < 3; i++) {
			assertArrayEquals(contentSegments[i], segments[i + 1]);
		}
	}
	
	@Test
	public void recvNoMessagesReady() {
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertFalse(_messenger.recv(_buffer, _defaultHeader, false));
		
		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void recvFailureOnFirstSegment() {
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		assertFalse(_defaultHeader.isValid(_buffer));
	}
	
	@Test
	public void recvFailureOnSubsequentSegment() {
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		assertFalse(_defaultHeader.isValid(_buffer));
	}
	
	@Test
	public void recvMessageTooLargeFirstSegment() {
		byte[] content1 = genContent(EVENT_BUFFER_LENGTH * 2);
		byte[] content2 = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(content1));
		when(_zmqSocket.recv(anyInt()))
			.thenReturn(content2);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		verify(_zmqSocket).recv();
		
		assertFalse(_defaultHeader.isValid(_buffer));
	}
	
	/*
	 * No way of telling this as jzmq transforms the return code to the truncated value using the standard send.
	 * Moving to recvZeroCopy would fix this as it returns the actual size of the segment
	 */
//	@Test
//	public void recvMessageTooLargeSubsequentSegment() {
//		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
//		byte[] content = genContent(EVENT_BUFFER_LENGTH * 2);
//		
//		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
//			.then(fakeRecv(expectedIdBytes));
//		when(_zmqSocket.recv(anyInt()))
//			.thenReturn(content);
//		when(_zmqSocket.hasReceiveMore())
//			.thenReturn(true)
//			.thenReturn(false);
//		
//		assertTrue(_receiver.recv(_socketPackage, _buffer));
//		
//		verify(_zmqSocket, times(2)).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
//		
//		assertFalse(_processingHeader.isValid(_buffer));
//	}
	
	@Test
	public void sendSingleSegment() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 1);
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		int segmentLength = 256;
		
		byte[] content = new byte[segmentLength];
		for (int i = 0; i < segmentLength; i += 4) {
			MessageBytesUtil.writeInt(content, i, i / 4);
		}
		
		int[] expectedOffsets = writeMessage(new byte[][] {content}, _buffer, header);		
		assertTrue(_messenger.send(_buffer, header, false));
		
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
		
		int[] expectedOffsets = writeMessage(contents, _buffer, header);
		assertTrue(_messenger.send(_buffer, header, false));
		
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
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		// write the segments out
		writeMessage(new byte[][] { new byte[0], new byte[0], new byte[0] }, _buffer, header);
		
		assertTrue(_messenger.send(_buffer, header, false));
	}
	
	@Test(expected=RuntimeException.class)
	public void sendSegmentLengthTooLargeForBuffer() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 3);
		
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
		
		writeMessage(contents, _buffer, header);
		int lastSegmentMetaData = header.getSegmentMetaData(_buffer, 2);
		int lastOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		header.setSegmentMetaData(_buffer, 2, lastOffset, EVENT_BUFFER_LENGTH);
		_messenger.send(_buffer, header, false);
	}
	
	@Test
	public void recvTimeOneSegment() {
		IncomingEventHeader header = new IncomingEventHeader(0, 1);
		_clock.setTime(10000, TimeUnit.MILLISECONDS);
		
		Answer<Integer> clockAdvancer = new Answer<Integer>() {

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				_clock.advance(1000, TimeUnit.MILLISECONDS);
				return 32;
			}
			
		};
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(clockAdvancer);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		assertEquals(11000, header.getRecvTime(_buffer));
	}
	
	@Test
	public void recvTimeMultipleSegments() {
		IncomingEventHeader header = new IncomingEventHeader(0, 4);
		_clock.setTime(10000, TimeUnit.MILLISECONDS);
		
		Answer<Integer> clockAdvancer = new Answer<Integer>() {

			@Override
			public Integer answer(InvocationOnMock invocation) throws Throwable {
				_clock.advance(1000, TimeUnit.MILLISECONDS);
				return 32;
			}
			
		};
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(clockAdvancer);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		assertEquals(11000, header.getRecvTime(_buffer));
	}
	
	@Test
	public void sentTimeOneSegment() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 1);
		header.setSegmentMetaData(_buffer, 0, header.getEventOffset(), 45);
		header.setIsValid(_buffer, true);
		_clock.setTime(10000, TimeUnit.MILLISECONDS);
		
		Answer<Boolean> clockAdvancer = new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				_clock.advance(1000, TimeUnit.MILLISECONDS);
				return true;
			}
			
		};
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(clockAdvancer)
			.then(clockAdvancer);

		assertTrue(_messenger.send(_buffer, header, false));
		assertEquals(11000, header.getSentTime(_buffer));
	}
	
	@Test
	public void sentTimeMultipleSegments() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 3);
		int cursor = header.getEventOffset();
		int length = (_buffer.length - header.getEventOffset()) / header.getSegmentCount();
		for (int i = 0; i < header.getSegmentCount(); i++) {
			header.setSegmentMetaData(_buffer, i, cursor, length);
			cursor += length;
		}
		header.setIsValid(_buffer, true);
		_clock.setTime(10000, TimeUnit.MILLISECONDS);
		
		Answer<Boolean> clockAdvancer = new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				_clock.advance(1000, TimeUnit.MILLISECONDS);
				return true;
			}
			
		};
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(clockAdvancer)
			.then(clockAdvancer)
			.then(clockAdvancer);

		assertTrue(_messenger.send(_buffer, header, false));
		assertEquals(13000, header.getSentTime(_buffer));
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
