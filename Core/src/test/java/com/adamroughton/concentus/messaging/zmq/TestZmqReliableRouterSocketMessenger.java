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

import static com.adamroughton.concentus.messaging.zmq.MessengerTestUtil.*;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.assertRangeEqual;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.doesNotHaveFlags;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.fakeRecv;
import static com.adamroughton.concentus.messaging.ZmqTestUtil.hasFlags;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.intThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
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
import com.adamroughton.concentus.messaging.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.messaging.ByteArrayCaptor;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;

@RunWith(MockitoJUnitRunner.class)
public class TestZmqReliableRouterSocketMessenger {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final IncomingEventHeader _defaultHeader = new IncomingEventHeader(0, 2);
	
	@Mock private ZMQ.Socket _zmqSocket;
	private ZmqSocketMessenger _messenger;
	private ArrayBackedResizingBuffer _buffer;
	private DrivableClock _clock;
	private final byte[] _headerFrame = new byte[ResizingBuffer.LONG_SIZE + ResizingBuffer.INT_SIZE];
	
	@Before
	public void setUp() {
		when(_zmqSocket.getType()).thenReturn(ZMQ.ROUTER);
		
		_buffer = new ArrayBackedResizingBuffer(EVENT_BUFFER_LENGTH);
		_clock = new DrivableClock();
		_clock.setTime(5500000, TimeUnit.MILLISECONDS);
		_messenger = new ZmqReliableRouterSocketMessenger(0, "", _zmqSocket, _clock, 1000, 2048, EVENT_BUFFER_LENGTH);
	}
	
	@Test
	public void recvSingleSegmentNoNack() {
		IncomingEventHeader header = new IncomingEventHeader(0, 2);
		
		byte[] content = genContent(256);
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[] headerFrame = makeMsgHeader(-1, content);
		
		MessageBytesUtil.writeLong(_headerFrame, 0, -1);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(headerFrame))
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
		byte[] headerFrame = makeMsgHeader(-1, contentSegments);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(headerFrame))
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
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		assertFalse(_defaultHeader.isValid(_buffer));
	}
	
	@Test
	public void recvFailureOnSubsequentSegment() {
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		assertFalse(_defaultHeader.isValid(_buffer));
	}
	
	@Test
	public void recvLargeMessage() {
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		byte[][] contents = new byte[2][];
		genSegmentData(EVENT_BUFFER_LENGTH * 2, 0, contents, 0, 1);		
		byte[] header = makeMsgHeader(-1, contents);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(header))
			.then(fakeRecv(contents[0]))
			.then(fakeRecv(contents[1]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		verify(_zmqSocket, times(4)).recv(any(byte[].class), anyInt(), anyInt(), anyInt());
		
		assertTrue(_defaultHeader.isValid(_buffer));
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
		OutgoingEventHeader header = new OutgoingEventHeader(0, 4);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		// write the segments out
		writeMessage(new byte[][] { expectedIdBytes, new byte[0], new byte[0], new byte[0] }, _buffer, header);
		
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
		ByteArrayCaptor eBytesCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		
		// set expectations
		int reliableMsgPartCount = (sentSegmentCount + 2) * reliableMsgCount;
		int totalSentSegmentCount = reliableMsgPartCount + (reliableMsgCount - nackSeq) * (sentSegmentCount + 2);
		Boolean[] returnList = new Boolean[totalSentSegmentCount - 1]; // -1 as thenReturn wants first arg separate from varargs
		for (int i = 0; i < returnList.length; i++) {
			returnList[i] = true;
		}
		when(_zmqSocket.send(argThat(eBytesCaptor), 
				eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt()))
				.thenReturn(true, returnList);
		
		final int segmentSize = 32;
		
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
			genSegmentData(segmentSize, 9 * i, messages[i], segmentIndices);
			
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
						frame = makeMsgHeader(i, sentSegmentCount * segmentSize);
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
		byte[] headerFrame = makeMsgHeader(nackSeq, 32);
		MessageBytesUtil.writeLong(_headerFrame, 0, nackSeq);
		IncomingEventHeader recvHeader = new IncomingEventHeader(0, 2);
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(idBytes))
			.then(fakeRecv(headerFrame))
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
	
	@Test
	public void nackMsg() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 2);
		byte[] idBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		long sentReliableSeq = sendSingleSegment(idBytes, header, true);
		
		// recv a NACK
		byte[] headerFrame = makeMsgHeader(sentReliableSeq, 32);
		IncomingEventHeader recvHeader = new IncomingEventHeader(0, 2);
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(idBytes))
			.then(fakeRecv(headerFrame));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		assertTrue(_messenger.recv(_buffer, recvHeader, false));
		assertFalse(recvHeader.isValid(_buffer));
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
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		sendSingleSegment(expectedIdBytes, header, isReliable);	
	}
	
	private long sendSingleSegment(byte[] idBytes, OutgoingEventHeader header, boolean isReliable) {
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
		
		ByteArrayCaptor eBytesCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		verify(_zmqSocket, times(3)).send(argThat(eBytesCaptor), eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt());
		
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
		byte[] expectedHeader = makeMsgHeader(isReliable? 0 : -1, content);
		byte[] seqBytes = eventParts.get(1);
		assertRangeEqual(expectedHeader, seqBytes, 0, 8);
		
		// contents
		byte[] messageBytes = eventParts.get(2);
		int actualContentOffset = offsets.get(2);
		assertEquals(expectedOffsets[1], actualContentOffset);
		int actualContentLength = (int) lengths.get(2);
		assertEquals(segmentLength, actualContentLength);
		assertRangeEqual(content, messageBytes, actualContentOffset, actualContentLength);	
		
		return 0;
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
		
		byte[] expectedHeader = makeMsgHeader(isReliable? 0 : -1, contents[1], contents[2], contents[3]);		
		int contentIndex = 0;
		for (int i = 0; i < 5; i++) {
			if (i == 1) {
				// check Seq frame
				assertEquals(12, (int) lengths.get(i));
				assertEquals(0, (int) offsets.get(i));
				assertRangeEqual(expectedHeader, eventParts.get(i), 0, 12);
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
	
}
