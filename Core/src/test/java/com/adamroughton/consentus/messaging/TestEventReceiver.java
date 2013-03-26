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

import java.util.Objects;
import java.util.UUID;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;

@RunWith(MockitoJUnitRunner.class)
public class TestEventReceiver {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final IncomingEventHeader _defaultHeader = new IncomingEventHeader(0, 2);
	
	@Mock private ZMQ.Socket _zmqSocket;
	private EventReceiver _defaultReceiver;
	private SocketPackage _socketPackage;
	private byte[] _buffer;
	
	@Before
	public void setUp() {
		_buffer = new byte[EVENT_BUFFER_LENGTH];
		_defaultReceiver = new EventReceiver(_defaultHeader, true);
		
		_socketPackage = SocketPackage.create(_zmqSocket)
				.setSocketId(0);
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
		EventReceiver receiver = new EventReceiver(header, true);
		
		byte[] content = genContent(256);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(receiver.recv(_socketPackage, _buffer));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(1, segments.length);
		assertArrayEquals(content, segments[0]);
	}
	
	@Test
	public void recvMultipleSegments() {
		IncomingEventHeader header = new IncomingEventHeader(0, 4);
		EventReceiver receiver = new EventReceiver(header, true);
		
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
		
		assertTrue(receiver.recv(_socketPackage, _buffer));
		
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
		
		assertFalse(_defaultReceiver.recv(_socketPackage, _buffer));
		
		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void recvFailureOnFirstSegment() {
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_defaultReceiver.recv(_socketPackage, _buffer));
		
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
		
		assertTrue(_defaultReceiver.recv(_socketPackage, _buffer));
		
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
		
		assertTrue(_defaultReceiver.recv(_socketPackage, _buffer));
		
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

}
