package com.adamroughton.consentus.clienthandler;

import java.util.Arrays;
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
public class TestOutgoingClientEventHandler {

	private RingBuffer<byte[]> _buffer;
	@Mock private ZMQ.Socket _zmqSocket;
	private OutgoingClientEventHandler _outgoingHandler;
	
	@Before
	public void setUp() {
		_buffer = new RingBuffer<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[512];
			}
		}, 4);
		_outgoingHandler = new OutgoingClientEventHandler(_buffer, _buffer.newBarrier());
		_buffer.setGatingSequences(_outgoingHandler.getSequence());
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
		MessageBytesUtil.writeUUID(outgoingBuffer, 1, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + 17, i / 4);
		}
		MessageBytesUtil.writeFlagToByte(outgoingBuffer, 0, 0, false);
		_buffer.publish(seq);
		
		assertTrue(_outgoingHandler.sendIfReady(_zmqSocket));
		
		ArgumentCaptor<byte[]> eBytesCaptor = ArgumentCaptor.forClass(byte[].class);
		verify(_zmqSocket).send(eBytesCaptor.capture(), eq(0), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(eBytesCaptor.capture(), eq(17), eq(ZMQ.NOBLOCK));
		
		List<byte[]> eventParts = eBytesCaptor.getAllValues();
		
		// id
		byte[] id = eventParts.get(0);
		assertEquals(expectedId, MessageBytesUtil.readUUID(id, 0));
		
		// contents
		byte[] contents = eventParts.get(1);
		for (int i = 0; i < 256; i += 4) {
			int expVal = i / 4;
			assertEquals(getUnmatchedOffsetMessage(expVal, i + 17, contents), 
					expVal, MessageBytesUtil.readInt(contents, i  + 17));
		}
		
		assertEquals(0, _outgoingHandler.getSequence().get());
	}
	
	@Test
	public void sendMultipleWithSocketAndBufferReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt()))
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
				MessageBytesUtil.writeInt(contents[i], j, -i);
			}
		}
		
		// write the events out
		for (int i = 0; i < 3; i++) {
			long seq = _buffer.next();
			byte[] outgoingBuffer = _buffer.get(seq);
			MessageBytesUtil.writeUUID(outgoingBuffer, 1, ids[i]);
			System.arraycopy(contents[i], 0, outgoingBuffer, 17, contents[i].length);
			_buffer.publish(seq);
		}
		
		ArgumentCaptor<byte[]> eventPartCaptor = ArgumentCaptor.forClass(byte[].class);
		for (int i = 0; i < 3; i++) {
			assertTrue(_outgoingHandler.sendIfReady(_zmqSocket));
		}
		
		verify(_zmqSocket, times(3)).send(eventPartCaptor.capture(), eq(0), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket, times(3)).send(eventPartCaptor.capture(), eq(17), eq(ZMQ.NOBLOCK));
		
		List<byte[]> eventParts = eventPartCaptor.getAllValues();
		assertEquals(6, eventParts.size());
		for (int i = 0; i < 3; i++) {
			byte[] idBytes = eventParts.get(2 * i);
			Arrays.equals(idBytes, idsBytes[i]);
			
			byte[] contentBytes = eventParts.get(2 * i + 1);
			arrayRangeEqual(contents[i], contentBytes, 17, contents[i].length);
		}
		
		assertEquals(2, _outgoingHandler.getSequence().get());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithSocketNotReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt()))
			.thenReturn(false);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, 1, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + 17, i / 4);
		}
		MessageBytesUtil.writeFlagToByte(outgoingBuffer, 0, 0, false);
		_buffer.publish(seq);
		
		assertFalse(_outgoingHandler.sendIfReady(_zmqSocket));
		
		verify(_zmqSocket, only()).send(argThat(hasBytes(expectedIdBytes)), eq(0), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		
		assertEquals(-1, _outgoingHandler.getSequence().get());
	}
	
	@Test
	public void sendWithSocketNotReadyThenReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt()))
			.thenReturn(false)
			.thenReturn(true)
			.thenReturn(true);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, 1, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + 17, i / 4);
		}
		MessageBytesUtil.writeFlagToByte(outgoingBuffer, 0, 0, false);
		_buffer.publish(seq);
		
		assertFalse(_outgoingHandler.sendIfReady(_zmqSocket));
		assertEquals(-1, _outgoingHandler.getSequence().get());
		
		assertTrue(_outgoingHandler.sendIfReady(_zmqSocket));
		verify(_zmqSocket, times(2)).send(argThat(hasBytes(expectedIdBytes)), eq(0), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(argThat(hasBytes(outgoingBuffer)), eq(17), eq(ZMQ.NOBLOCK));
		
		assertEquals(0, _outgoingHandler.getSequence().get());
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithNoPendingEvents() {
		_outgoingHandler.sendIfReady(_zmqSocket);
		assertEquals(-1, _outgoingHandler.getSequence().get());
		verifyZeroInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithNoPendingEventsThenPendingEvents() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		_outgoingHandler.sendIfReady(_zmqSocket);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		
		// write the ID
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(outgoingBuffer, 1, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + 17, i / 4);
		}
		MessageBytesUtil.writeFlagToByte(outgoingBuffer, 0, 0, false);
		_buffer.publish(seq);
		
		_outgoingHandler.sendIfReady(_zmqSocket);
		
		verify(_zmqSocket).send(argThat(hasBytes(expectedIdBytes)), eq(0), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(argThat(hasBytes(outgoingBuffer)), eq(17), eq(ZMQ.NOBLOCK));
		
		assertEquals(0, _outgoingHandler.getSequence().get());
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
		MessageBytesUtil.writeUUID(outgoingBuffer, 1, expectedId);
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(outgoingBuffer, i + 17, i / 4);
		}
		MessageBytesUtil.writeFlagToByte(outgoingBuffer, 0, 0, false);
		_buffer.publish(seq);
		
		assertFalse(_outgoingHandler.sendIfReady(_zmqSocket));
		verify(_zmqSocket).send(argThat(hasBytes(expectedIdBytes)), eq(0), eq(ZMQ.SNDMORE | ZMQ.NOBLOCK));
		verify(_zmqSocket).send(argThat(hasBytes(outgoingBuffer)), eq(17), eq(ZMQ.NOBLOCK));
		assertEquals(-1, _outgoingHandler.getSequence().get());		
		
		verifyNoMoreInteractions(_zmqSocket);
	}
	
	private String getUnmatchedOffsetMessage(int expectedValue, int offset, byte[] actual) {
		return String.format("expected: %d at offset %d, actual array: %s", 
				expectedValue, 
				offset, 
				toHexStringSegment(actual, offset, 5));
	}
	
}
