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
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.concentus.DrivableClock;
import com.adamroughton.concentus.messaging.ByteArrayCaptor;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

@RunWith(MockitoJUnitRunner.class)
public class TestZmqReliableRouterSocketMessenger {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final IncomingEventHeader _defaultHeader = new IncomingEventHeader(0, 2);
	
	@Mock private ZMQ.Socket _zmqSocket;
	private ZmqSocketMessenger _messenger;
	private byte[] _buffer;
	private DrivableClock _clock;
	private final byte[] _nackSeqFrame = new byte[8];
	
	@Before
	public void setUp() {
		when(_zmqSocket.getType()).thenReturn(ZMQ.ROUTER);
		
		_buffer = new byte[EVENT_BUFFER_LENGTH];
		_clock = new DrivableClock();
		_clock.setTime(5500000, TimeUnit.MILLISECONDS);
		_messenger = new ZmqReliableRouterSocketMessenger(0, "", _zmqSocket, _clock, 1000, 2048, EVENT_BUFFER_LENGTH);
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
	
	private void genSegmentData(int length, int seed, byte[][] segments, int...indices) {
		for (int index : indices) {
			if (index < 0 || index >= segments.length) 
				throw new IllegalArgumentException(String.format("Provided index '%d' out of bounds [%d, %d]", index, 0, segments.length));
			segments[index] = genContent(length, seed++);
		}
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
	public void recvSingleSegmentNoNack() {
		IncomingEventHeader header = new IncomingEventHeader(0, 2);
		
		byte[] content = genContent(256);
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		MessageBytesUtil.writeLong(_nackSeqFrame, 0, -1);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(_nackSeqFrame))
			.then(fakeRecv(content));
		
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(2, segments.length);
		assertArrayEquals(expectedIdBytes, segments[0]);
		assertArrayEquals(content, segments[1]);
	}
	
	@Test
	public void recvMultipleSegmentsNoNack() {
		IncomingEventHeader header = new IncomingEventHeader(0, 4);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[][] contentSegments = new byte[3][];
		for (int i = 0; i < 3; i++) {
			contentSegments[i] = genContent(32, i);
		}
		MessageBytesUtil.writeLong(_nackSeqFrame, 0, -1);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(_nackSeqFrame))
			.then(fakeRecv(contentSegments[0]))
			.then(fakeRecv(contentSegments[1]))
			.then(fakeRecv(contentSegments[2]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(4, segments.length);
		
		assertArrayEquals(expectedIdBytes, segments[0]);
		for (int i = 0; i < 3; i++) {
			assertArrayEquals(String.format("Segment %d did not match", i), contentSegments[i], segments[i + 1]);
		}
	}
	
	@Test
	public void recvNoMessagesReady() {
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertFalse(_messenger.recv(_buffer, _defaultHeader, false));
		
		verify(_zmqSocket).getType();
		verify(_zmqSocket).recv(any(byte[].class), anyInt(), anyInt(), anyInt());
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
	
	@Test
	public void sendSingleSegmentUnreliable() {
		sendSingleSegment(false);	
	}
	
	@Test
	public void sendMultipleSegmentsUnreliable() {
		sendMultipleSegments(false);
	}	
	
	@Test
	public void sendSingleSegmentReliable() {
		sendSingleSegment(true);	
	}
	
	@Test
	public void sendMultipleSegmentsReliable() {
		sendMultipleSegments(true);
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

		byte[][] contents = new byte[3][];
		genSegmentData(32, 5, contents, 0, 1, 2);
		
		writeMessage(contents, _buffer, header);
		int lastSegmentMetaData = header.getSegmentMetaData(_buffer, 2);
		int lastOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		header.setSegmentMetaData(_buffer, 2, lastOffset, EVENT_BUFFER_LENGTH);
		_messenger.send(_buffer, header, false);
	}
	
	@Test
	public void nackOnFirst() {
		nack(0, 5, 3);
	}
	
	@Test
	public void nackOnLast() {
		nack(4, 5, 3);
	}
	
	@Test
	public void nackOnMiddle() {
		nack(2, 5, 3);
	}
	
	private void nack(int nackSeq, int reliableMsgCount, int sentSegmentCount) {
		byte[] idBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		OutgoingEventHeader sendHeader = new OutgoingEventHeader(0, sentSegmentCount + 1);
		
		// send some reliable messages
		ArgumentCaptor<byte[]> eBytesCaptor = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		
		// set expectations
		int reliableMsgPartCount = (sentSegmentCount + 2) * reliableMsgCount;
		int totalSentSegmentCount = reliableMsgPartCount + (reliableMsgCount - nackSeq) * (sentSegmentCount + 2);
		Boolean[] returnList = new Boolean[totalSentSegmentCount - 1]; // -1 as thenReturn wants first arg separate from varargs
		for (int i = 0; i < returnList.length; i++) {
			returnList[i] = true;
		}
		when(_zmqSocket.send(eBytesCaptor.capture(), 
				eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt()))
				.thenReturn(true, returnList);
		
		byte[][][] messages = new byte[reliableMsgCount][][];
		byte[][][] expectedFrames = new byte[reliableMsgCount - nackSeq][][];
		for (int i = 0; i < reliableMsgCount; i++) {
			messages[i] = new byte[sentSegmentCount + 1][];
			messages[i][0] = idBytes;
			
			// segmentIndices = (1 to sendSegmentCount + 1)
			int[] segmentIndices = new int[sentSegmentCount];
			for (int j = 0; j < sentSegmentCount; j++) {
				segmentIndices[j] = j + 1;
			}
			
			// generate data for segments (excl. first which is ID)
			genSegmentData(32, 9 * i, messages[i], segmentIndices);
			
			// send reliable messgae
			writeMessage(messages[i], _buffer, sendHeader);
			sendHeader.setIsReliable(_buffer, true);
			assertTrue("Test error - should send ok", _messenger.send(_buffer, sendHeader, false));
			
			// -- set expectations --
			
			if (i >= nackSeq) {
				// sentSegmentCount + 2 to account for both ID and Seq frame
				expectedFrames[i - nackSeq] = new byte[sentSegmentCount + 2][];
				int frameIndex = 0;
				for (int j = 0; j < sentSegmentCount + 2; j ++) {
					byte[] frame;
					if (j == 1) {
						// seq frame
						frame = new byte[8];
						MessageBytesUtil.writeLong(frame, 0, i);
					} else {
						int length = messages[i][frameIndex].length;
						frame = new byte[length];
						System.arraycopy(messages[i][frameIndex], 0, frame, 0, length);	
						frameIndex++;
					}
					expectedFrames[i - nackSeq][j] = frame;
				}
			}
		}
		
		// recv a NACK
		MessageBytesUtil.writeLong(_nackSeqFrame, 0, nackSeq);
		IncomingEventHeader recvHeader = new IncomingEventHeader(0, 2);
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(idBytes))
			.then(fakeRecv(_nackSeqFrame))
			.then(fakeRecv(new byte[32]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		assertTrue("Test error - should recv ok", _messenger.recv(_buffer, recvHeader, false));
		
		// verify events resent by checking content
		List<byte[]> sentFrames = eBytesCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
		
		assertEquals(totalSentSegmentCount, sentFrames.size());
		
		for (int i = 0; i < reliableMsgCount - nackSeq; i++) {
			// sentSegmentCount + 2 to account for both ID and Seq frame
			for (int segmentIndex = 0; segmentIndex < sentSegmentCount + 2; segmentIndex++) {
				int indexOfPart = reliableMsgPartCount + (sentSegmentCount + 2) * i + segmentIndex;
				
				byte[] sentFrame = sentFrames.get(indexOfPart);
				int sentOffset = offsets.get(indexOfPart);
				int sentLength = lengths.get(indexOfPart);
				
				String assertMsg = String.format("msgIndex: %d, segmentIndex: %d", i, segmentIndex);
				assertEquals(assertMsg,	expectedFrames[i][segmentIndex].length, sentLength);
				assertRangeEqual(assertMsg, expectedFrames[i][segmentIndex], 
						sentFrame,
						sentOffset,
						sentLength);
			}
		}
	}
	

	
	public void nackNoCorrespondingClient() {
		
	}
	
	public void nackOutOfRange() {
		
	}
	
	@Test
	public void subsequentResend() {
		
	}
	
	
	private void sendSingleSegment(boolean isReliable) {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 2);
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		int segmentLength = 256;
		
		byte[] content = genContent(segmentLength);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		int[] expectedOffsets = writeMessage(new byte[][] {expectedIdBytes, content}, _buffer, header);
		header.setIsReliable(_buffer, isReliable);
		
		assertTrue(_messenger.send(_buffer, header, false));
		assertFalse(header.isPartiallySent(_buffer));
		
		ArgumentCaptor<byte[]> eBytesCaptor = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		verify(_zmqSocket, times(3)).send(eBytesCaptor.capture(), eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt());
		
		List<byte[]> eventParts = eBytesCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
			
		// id
		byte[] actualIdBytes = eventParts.get(0);
		int actualIdOffset = offsets.get(0);
		assertEquals(expectedOffsets[0], actualIdOffset);
		int actualIdLength = (int) lengths.get(0);
		assertEquals(expectedIdBytes.length, actualIdLength);
		assertRangeEqual(expectedIdBytes, actualIdBytes, actualIdOffset, actualIdLength);
		
		// seq frame
		MessageBytesUtil.writeLong(_nackSeqFrame, 0, isReliable? 0 : -1);
		byte[] seqBytes = eventParts.get(1);
		assertRangeEqual(_nackSeqFrame, seqBytes, 0, 8);
		
		// contents
		byte[] messageBytes = eventParts.get(2);
		int actualContentOffset = offsets.get(2);
		assertEquals(expectedOffsets[1], actualContentOffset);
		int actualContentLength = (int) lengths.get(2);
		assertEquals(segmentLength, actualContentLength);
		assertRangeEqual(content, messageBytes, actualContentOffset, actualContentLength);	
	}
	
	private void sendMultipleSegments(boolean isReliable) {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 4);
		
		ByteArrayCaptor eventPartCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		when(_zmqSocket.send(argThat(eventPartCaptor), eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[][] contents = new byte[4][];
		contents[0] = expectedIdBytes;
		genSegmentData(32, 7, contents, 1, 2, 3);
		
		int[] expectedOffsets = writeMessage(contents, _buffer, header);
		header.setIsReliable(_buffer, isReliable);
		assertTrue(_messenger.send(_buffer, header, true));
		assertFalse(header.isPartiallySent(_buffer));
		
		verify(_zmqSocket, times(4)).send(any(byte[].class), anyInt(), anyInt(), intThat(hasFlags(ZMQ.SNDMORE)));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), anyInt(), intThat(doesNotHaveFlags(ZMQ.SNDMORE)));
		
		List<byte[]> eventParts = eventPartCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
		assertEquals(5, eventParts.size());
		
		MessageBytesUtil.writeLong(_nackSeqFrame, 0, isReliable? 0 : -1);
		int contentIndex = 0;
		for (int i = 0; i < 5; i++) {
			if (i == 1) {
				// check Seq frame
				assertEquals(8, (int) lengths.get(i));
				assertEquals(0, (int) offsets.get(i));
				assertRangeEqual(_nackSeqFrame, eventParts.get(i), 0, 8);
			} else {
				int actualLength = (int)lengths.get(i);
				assertEquals(contents[contentIndex].length, actualLength);
				int actualOffset = (int)offsets.get(i);
				assertEquals(expectedOffsets[contentIndex], actualOffset);
				byte[] contentBytes = eventParts.get(i);
				assertRangeEqual(contents[contentIndex], contentBytes, actualOffset, actualLength);
				contentIndex++;
			}
		}
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
