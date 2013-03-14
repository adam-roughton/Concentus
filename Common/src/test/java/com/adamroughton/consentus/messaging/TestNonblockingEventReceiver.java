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

import java.util.UUID;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.NonblockingEventReceiver;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;

@RunWith(MockitoJUnitRunner.class)
public class TestNonblockingEventReceiver {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final EventProcessingHeader _processingHeader = new EventProcessingHeader(0, 1);
	private final int _msgOffset = _processingHeader.getEventOffset();
	
	private RingBuffer<byte[]> _buffer;
	private Sequence _gatingSeq = new Sequence(-1);
	@Mock private ZMQ.Socket _zmqSocket;
	private NonblockingEventReceiver _receiver;
	private SocketPackage _socketPackage;
	
	@Before
	public void setUp() {
		_buffer = new RingBuffer<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[EVENT_BUFFER_LENGTH];
			}
		}, 4);
		_buffer.setGatingSequences(_gatingSeq);
		
		// fake publish to get to wrap around point
		for (int i = 0; i < 4; i++) {
			long seq = _buffer.next();
			_buffer.publish(seq);
		}
		// gating seq set such that no buffer space is available
		_gatingSeq.set(-1);
		_receiver = new NonblockingEventReceiver(_buffer, _processingHeader);
		
		_socketPackage = SocketPackage.create(_zmqSocket)
				.setMessageOffsets(0, 16)
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
	
	@Test
	public void recvWithAvailableSpace() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[] content = genContent(256);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(_processingHeader.isValid(incomingEvent));
		assertRangeEqual(expectedIdBytes, incomingEvent, _msgOffset, expectedIdBytes.length);
		assertRangeEqual(content, incomingEvent, _msgOffset + 16, content.length);
	}
	
	@Test
	public void recvWithNoBufferSpace() {
		_gatingSeq.set(-1);
		
		assertFalse(_receiver.recvIfReady(_socketPackage));
		verify(_zmqSocket, never()).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		
		assertEquals(3, _buffer.getCursor());
	}
	
	@Test
	public void recvNoMessagesReady() {
		_gatingSeq.set(0);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenReturn(0);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(3, _buffer.getCursor());
		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void recvWithNoBufferSpaceThenRecvWithSpace() {
		_gatingSeq.set(-1);
		
		assertFalse(_receiver.recvIfReady(_socketPackage));
		verify(_zmqSocket, never()).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		assertEquals(3, _buffer.getCursor());
		
		_gatingSeq.set(3);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[] content = genContent(256);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(_processingHeader.isValid(incomingEvent));
		assertRangeEqual(expectedIdBytes, incomingEvent, _msgOffset, expectedIdBytes.length);
		assertRangeEqual(content, incomingEvent, _msgOffset + 16, content.length);
	}
	
	@Test
	public void recvFailureOnIdentity() {
		_gatingSeq.set(0);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		assertFalse(_processingHeader.isValid(incomingEvent));
	}
	
	@Test
	public void recvFailureOnMessage() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertFalse(_processingHeader.isValid(incomingEvent));
		assertRangeEqual(expectedIdBytes, incomingEvent, _msgOffset, expectedIdBytes.length);
		assertRangeEqual(new byte[256], incomingEvent, _msgOffset + 16, 256);
	}
	
	@Test
	public void recvNotEnoughParts() {
		_gatingSeq.set(0);
		
		byte[] idBytes = new byte[2];		
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(idBytes));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertFalse(_processingHeader.isValid(incomingEvent));
	}
	
	@Test
	public void recvTooManyMessageParts() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[] content1 = genContent(256);
		byte[] content2 = genContent(256, 1);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(content1));
		/**
		 * we expect the event handler to wait for all of the remaining parts so that it
		 * is in a valid state on the next call.
		 */
		when(_zmqSocket.recv(0))
			.thenReturn(content2);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		_receiver.recvIfReady(_socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(_processingHeader.isValid(incomingEvent));
		assertRangeEqual(expectedIdBytes, incomingEvent, _msgOffset, expectedIdBytes.length);
		assertRangeEqual(content1, incomingEvent, _msgOffset + 16, content1.length);
	}
	
	// not possible anymore due to the way SocketPackage is written
//	@Test
//	public void recvZeroOffsets() {
//		_gatingSeq.set(0);
//		
//		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
//		byte[] content = genContent(256);
//		
//		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
//			.then(fakeRecv(expectedIdBytes));
//		when(_zmqSocket.recv(anyInt()))
//			.thenReturn(content);
//		when(_zmqSocket.hasReceiveMore())
//			.thenReturn(true)
//			.thenReturn(true) // second call for same part
//			.thenReturn(false);
//		
//		
//		_receiver.recvIfReady(_zmqSocket, new MessagePartBufferPolicy(), 0);
//		
//		assertEquals(4, _buffer.getCursor());
//		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
//		verify(_zmqSocket).recv(anyInt());
//		
//		byte[] incomingEvent = _buffer.get(0);
//		assertTrue(_processingHeader.isValid(incomingEvent));
//	}
	
//	@Test
//	public void recvZeroOffsetsNoMessagesReady() {
//		_gatingSeq.set(0);
//		
//		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
//			.thenReturn(0);
//		when(_zmqSocket.hasReceiveMore())
//			.thenReturn(false);
//		
//		_receiver.recvIfReady(_zmqSocket, new MessagePartBufferPolicy(), 0);
//		
//		assertEquals(3, _buffer.getCursor());
//		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
//		verifyNoMoreInteractions(_zmqSocket);
//	}
	
	@Test
	public void recvReqBufferTooLarge() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[] content = genContent(256);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes));
		when(_zmqSocket.recv(anyInt()))
			.thenReturn(content);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		SocketPackage socketPackage = SocketPackage.create(_zmqSocket)
				.setMessageOffsets(EVENT_BUFFER_LENGTH, EVENT_BUFFER_LENGTH + 16)
				.setSocketId(0);
		
		_receiver.recvIfReady(socketPackage);
		
		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		verify(_zmqSocket).recv(anyInt());
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertFalse(_processingHeader.isValid(incomingEvent));
	}
	
	@Test
	public void recvReqBufferTooLargeNoMessagesReady() {
		_gatingSeq.set(0);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenReturn(0);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		SocketPackage socketPackage = SocketPackage.create(_zmqSocket)
				.setMessageOffsets(EVENT_BUFFER_LENGTH, EVENT_BUFFER_LENGTH + 16)
				.setSocketId(0);
		
		_receiver.recvIfReady(socketPackage);
		
		assertEquals(3, _buffer.getCursor());
		verify(_zmqSocket).recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void recvSingleOffset() {
		_gatingSeq.set(0);
		
		byte[] content = genContent(256);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		SocketPackage socketPackage = SocketPackage.create(_zmqSocket)
				.setMessageOffsets(0)
				.setSocketId(0);
		
		_receiver.recvIfReady(socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(_processingHeader.isValid(incomingEvent));
		assertRangeEqual(content, incomingEvent, _msgOffset, content.length);
	}
	
	@Test
	public void recvMultipleOffsets() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = genIdBytes(UUID.fromString("abababab-abab-abab-abab-abababababab"));
		byte[] content1 = genContent(32);
		byte[] content2 = genContent(32, 1);
		byte[] content3 = genContent(32, 2);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(content1))
			.then(fakeRecv(content2))
			.then(fakeRecv(content3));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		SocketPackage socketPackage = SocketPackage.create(_zmqSocket)
				.setMessageOffsets(0, 16, 48, 80)
				.setSocketId(0);
		
		_receiver.recvIfReady(socketPackage);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(_processingHeader.isValid(incomingEvent));
		assertRangeEqual(expectedIdBytes, incomingEvent, _msgOffset, expectedIdBytes.length);
		assertRangeEqual(content1, incomingEvent, _msgOffset + 16, content1.length);
		assertRangeEqual(content2, incomingEvent, _msgOffset + 48, content2.length);
		assertRangeEqual(content3, incomingEvent, _msgOffset + 80, content3.length);
	}
}
