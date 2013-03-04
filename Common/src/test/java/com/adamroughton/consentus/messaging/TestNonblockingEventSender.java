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
import java.util.UUID;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.Util.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;

@RunWith(MockitoJUnitRunner.class)
public class TestNonblockingEventSender {

	private static final int BUFFER_SIZE = 512;
	
	private RingBuffer<byte[]> _buffer;
	@Mock private ZMQ.Socket _zmqSocket;
	private NonblockingEventSender _sender;
	private final EventProcessingHeader _processingHeader = new EventProcessingHeader(0, 1);
	private final MessagePartBufferPolicy _clientMsgOffsets = new MessagePartBufferPolicy(0, 16);
	
	private final int _msgOffset = _processingHeader.getEventOffset();
	
	@Before
	public void setUp() {
		_buffer = new RingBuffer<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[BUFFER_SIZE];
			}
		}, 4);
		_sender = new NonblockingEventSender(_buffer, _buffer.newBarrier(), _processingHeader);
		_buffer.setGatingSequences(_sender.getSequence());
	}
	
	@Test
	public void sendWithSocketAndBufferReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + _msgOffset + 16, i / 4);
		}
		_processingHeader.setIsValid(true, outgoingBuffer);
		_buffer.publish(seq);
		
		assertTrue(_sender.sendIfReady(_zmqSocket, _clientMsgOffsets));
		
		ArgumentCaptor<byte[]> eBytesCaptor = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		verify(_zmqSocket).send(eBytesCaptor.capture(), eOffsetCaptor.capture(), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(eBytesCaptor.capture(), eOffsetCaptor.capture(), eq(ZMQ.NOBLOCK));
		
		List<byte[]> eventParts = eBytesCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		
		// id
		byte[] id = eventParts.get(0);
		assertEquals(expectedId, MessageBytesUtil.readUUID(id, offsets.get(0)));
		
		// contents
		byte[] contents = eventParts.get(1);
		int contentOffset = offsets.get(1);
		for (int i = 0; i < 256; i += 4) {
			int expVal = i / 4;
			assertEquals(getUnmatchedOffsetMessage(expVal, i + contentOffset, contents), 
					expVal, MessageBytesUtil.readInt(contents, i  + contentOffset));
		}
		
		assertEquals(0, _sender.getSequence().get());
	}
	
	@Test
	public void sendMultipleWithSocketAndBufferReady() {
		ByteArrayCaptor eventPartCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		when(_zmqSocket.send(argThat(eventPartCaptor), eOffsetCaptor.capture(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		UUID id1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
		UUID id2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
		UUID id3 = UUID.fromString("33333333-3333-3333-3333-333333333333");
		UUID[] ids = new UUID[] {id1, id2, id3};
		
		byte[] id1Bytes = new byte[16];
		MessageBytesUtil.writeUUID(id1Bytes, 0, id1);
		byte[] id2Bytes = new byte[16];
		MessageBytesUtil.writeUUID(id2Bytes, 0, id2);
		byte[] id3Bytes = new byte[16];
		MessageBytesUtil.writeUUID(id3Bytes, 0, id3);
		byte[][] idsBytes = new byte[][] {id1Bytes, id2Bytes, id3Bytes};
		
		byte[] content1 = new byte[256];
		byte[] content2 = new byte[256];
		byte[] content3 = new byte[256];
		byte[][] contents = new byte[][] {content1, content2, content3};
		
		for (int i = 0; i < contents.length; i++) {
			for (int j = 0; j < 256; j += 4) {
				MessageBytesUtil.writeInt(contents[i], j, -(i + 1));
			}
		}
		
		// write the events out
		for (int i = 0; i < 3; i++) {
			long seq = _buffer.next();
			byte[] outgoingBuffer = _buffer.get(seq);
			MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, ids[i]);
			System.arraycopy(contents[i], 0, outgoingBuffer, _msgOffset + 16, contents[i].length);
			_processingHeader.setIsValid(true, outgoingBuffer);
			_buffer.publish(seq);
		}
		
		for (int i = 0; i < 3; i++) {
			assertTrue(_sender.sendIfReady(_zmqSocket, _clientMsgOffsets));
		}
		
		verify(_zmqSocket, times(3)).send(any(byte[].class), anyInt(), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket, times(3)).send(any(byte[].class), anyInt(), eq(ZMQ.NOBLOCK));
		
		List<byte[]> eventParts = eventPartCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		assertEquals(6, eventParts.size());
		for (int i = 0; i < 3; i++) {
			byte[] idBytes = eventParts.get(2 * i);
			int idOffset = offsets.get(2 * 1);
			assertRangeEqual(idsBytes[i], idBytes, idOffset, idsBytes[i].length);
			
			byte[] contentBytes = eventParts.get(2 * i + 1);
			int contentOffset = offsets.get(2 * i + 1);
			assertRangeEqual(contents[i], contentBytes, contentOffset, contents[i].length);
		}
		
		assertEquals(2, _sender.getSequence().get());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithSocketNotReady() {
		ByteArrayCaptor eventPartCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		
		when(_zmqSocket.send(argThat(eventPartCaptor), eOffsetCaptor.capture(), anyInt()))
			.thenReturn(false);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + _msgOffset + 16, i / 4);
		}
		_processingHeader.setIsValid(true, outgoingBuffer);
		_buffer.publish(seq);
		
		assertFalse(_sender.sendIfReady(_zmqSocket, _clientMsgOffsets));
		verify(_zmqSocket, only()).send(any(byte[].class), anyInt(), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		
		assertRangeEqual(expectedIdBytes, eventPartCaptor.getValue(), eOffsetCaptor.getValue(), expectedIdBytes.length);
		
		assertEquals(-1, _sender.getSequence().get());
	}
	
	@Test
	public void sendWithSocketNotReadyThenReady() {
		ByteArrayCaptor eventPartCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		
		when(_zmqSocket.send(argThat(eventPartCaptor), eOffsetCaptor.capture(), anyInt()))
			.thenReturn(false)
			.thenReturn(true)
			.thenReturn(true);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		byte[] content = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + _msgOffset + 16, i / 4);
			MessageBytesUtil.writeInt(content, i, i / 4);
		}
		_processingHeader.setIsValid(true, outgoingBuffer);
		_buffer.publish(seq);
		
		assertFalse(_sender.sendIfReady(_zmqSocket, _clientMsgOffsets));
		assertEquals(-1, _sender.getSequence().get());
		
		assertTrue(_sender.sendIfReady(_zmqSocket, _clientMsgOffsets));
		verify(_zmqSocket, times(2)).send(any(byte[].class), anyInt(), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), eq(ZMQ.NOBLOCK));
		
		assertEquals(0, _sender.getSequence().get());
		
		List<byte[]> sentBytes = eventPartCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		
		// id, id, content
		assertRangeEqual(expectedIdBytes, sentBytes.get(0), offsets.get(0), expectedIdBytes.length);
		assertRangeEqual(expectedIdBytes, sentBytes.get(1), offsets.get(1), expectedIdBytes.length);
		assertRangeEqual(content, sentBytes.get(2), offsets.get(2), content.length);
				
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithNoPendingEvents() {
		_sender.sendIfReady(_zmqSocket, _clientMsgOffsets);
		assertEquals(-1, _sender.getSequence().get());
		verifyZeroInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithNoPendingEventsThenPendingEvents() {
		ByteArrayCaptor eventPartCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		
		when(_zmqSocket.send(argThat(eventPartCaptor), eOffsetCaptor.capture(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		_sender.sendIfReady(_zmqSocket, _clientMsgOffsets);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		byte[] content = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + _msgOffset + 16, i / 4);
			MessageBytesUtil.writeInt(content, i, i / 4);
		}
		_processingHeader.setIsValid(true, outgoingBuffer);
		_buffer.publish(seq);
		
		_sender.sendIfReady(_zmqSocket, _clientMsgOffsets);
		
		verify(_zmqSocket).send(any(byte[].class), anyInt(), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), eq(ZMQ.NOBLOCK));
		
		List<byte[]> sentBytes = eventPartCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		
		// id, id, content
		assertRangeEqual(expectedIdBytes, sentBytes.get(0), offsets.get(0), expectedIdBytes.length);
		assertRangeEqual(content, sentBytes.get(1), offsets.get(1), content.length);
		
		assertEquals(0, _sender.getSequence().get());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithSocketNotReadyForSecondPart() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(false);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + _msgOffset + 16, i / 4);
		}
		_processingHeader.setIsValid(true, outgoingBuffer);
		_buffer.publish(seq);
		
		assertFalse(_sender.sendIfReady(_zmqSocket, _clientMsgOffsets));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), eq(ZMQ.NOBLOCK));
		assertEquals(-1, _sender.getSequence().get());	
		
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void sendWithOffsetOutOfRange() {
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, _msgOffset, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + _msgOffset + 16, i / 4);
		}
		_processingHeader.setIsValid(true, outgoingBuffer);
		_buffer.publish(seq);
		
		_sender.sendIfReady(_zmqSocket, new MessagePartBufferPolicy(BUFFER_SIZE));
	}
	
	private String getUnmatchedOffsetMessage(int expectedValue, int offset, byte[] actual) {
		return String.format("expected: %d at offset %d, actual array: %s", 
				expectedValue, 
				offset, 
				toHexStringSegment(actual, offset, 5));
	}
	
}
