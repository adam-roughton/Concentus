package com.adamroughton.consentus.clienthandler;

import java.util.UUID;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;
import static com.adamroughton.consentus.Util.*;

@RunWith(MockitoJUnitRunner.class)
public class TestIncomingClientEventHandler {

	private RingBuffer<byte[]> _buffer;
	private Sequence _gatingSeq = new Sequence(-1);
	@Mock private ZMQ.Socket _zmqSocket;
	private IncomingClientEventHandler _incomingHandler;
	
	@Before
	public void setUp() {
		_buffer = new RingBuffer<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[512];
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
		_incomingHandler = new IncomingClientEventHandler(_buffer);
	}
	
	@Test
	public void recvWithAvailableSpace() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		byte[] content = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(content, i, i / 4);
		}
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertFalse(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
		assertEquals(expectedId, MessageBytesUtil.readUUID(incomingEvent, 1));
		for (int i = 0; i < 256; i += 4) {
			int expVal = i / 4;
			assertEquals(getUnmatchedOffsetMessage(expVal, i + 17, incomingEvent), 
					expVal, MessageBytesUtil.readInt(incomingEvent, i + 17));
		}
	}
	
	@Test
	public void recvWithNoBufferSpace() {
		_gatingSeq.set(-1);
		
		assertFalse(_incomingHandler.recvIfReady(_zmqSocket));
		verify(_zmqSocket, never()).recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt());
		
		assertEquals(3, _buffer.getCursor());
	}
	
	@Test
	public void recvWithNoBufferSpaceThenRecvWithSpace() {
		_gatingSeq.set(-1);
		
		assertFalse(_incomingHandler.recvIfReady(_zmqSocket));
		verify(_zmqSocket, never()).recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt());
		assertEquals(3, _buffer.getCursor());
		
		_gatingSeq.set(3);
		
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		byte[] content = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(content, i, i / 4);
		}
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertFalse(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
		assertEquals(expectedId, MessageBytesUtil.readUUID(incomingEvent, 1));
		for (int i = 0; i < 256; i += 4) {
			int expVal = i / 4;
			assertEquals(getUnmatchedOffsetMessage(expVal, i + 17, incomingEvent), 
					expVal, MessageBytesUtil.readInt(incomingEvent, i + 17));
		}
	}
	
	@Test
	public void recvFailureOnIdentity() {
		_gatingSeq.set(0);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		assertTrue(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
	}
	
	@Test
	public void recvFailureOnMessage() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(expectedIdBytes))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
		assertEquals(expectedId, MessageBytesUtil.readUUID(incomingEvent, 1));
		for (int i = 0; i < 256; i += 4) {
			assertEquals(0, MessageBytesUtil.readInt(incomingEvent, i + 17));
		}
	}
	
	@Test
	public void recvIdTooShort() {
		_gatingSeq.set(0);
		
		byte[] idBytes = new byte[2];		
		byte[] content = new byte[256];
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(idBytes));
		when(_zmqSocket.recv(anyInt()))
		.thenReturn(content);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
	}
	
	@Test
	public void recvNotEnoughParts() {
		_gatingSeq.set(0);
		
		byte[] idBytes = new byte[2];		
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(idBytes));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertTrue(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
	}
	
	@Test
	public void recvTooManyMessageParts() {
		_gatingSeq.set(0);
		
		byte[] expectedIdBytes = new byte[16];
		UUID expectedId = UUID.fromString("abababab-abab-abab-abab-abababababab");
		MessageBytesUtil.writeUUID(expectedIdBytes, 0, expectedId);
		
		byte[] content1 = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(content1, i, i / 4);
		}
		
		byte[] content2 = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(content2, i, 0xFFFFFFFF);
		}
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[512])), anyInt(), anyInt(), anyInt()))
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
		
		_incomingHandler.recvIfReady(_zmqSocket);
		
		assertEquals(4, _buffer.getCursor());
		byte[] incomingEvent = _buffer.get(0);
		
		assertFalse(MessageBytesUtil.readFlagFromByte(incomingEvent, 0, 0));
		assertEquals(expectedId, MessageBytesUtil.readUUID(incomingEvent, 1));
		for (int i = 0; i < 256; i += 4) {
			int expVal = i / 4;
			assertEquals(getUnmatchedOffsetMessage(expVal, i + 17, incomingEvent), 
					expVal, MessageBytesUtil.readInt(incomingEvent, i + 17));
		}
	}
	
	private String getUnmatchedOffsetMessage(int expectedValue, int offset, byte[] actual) {
		return String.format("expected: %d at offset %d, actual array: %s", 
				expectedValue, 
				offset, 
				toHexStringSegment(actual, offset, 5));
	}
	
}
