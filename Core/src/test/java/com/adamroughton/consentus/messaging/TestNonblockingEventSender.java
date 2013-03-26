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

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestNonblockingEventSender {

	private static final int BUFFER_SIZE = 512;
	
	private RingBuffer<byte[]> _buffer;
	@Mock private ZMQ.Socket _zmqSocket;
	private NonblockingEventSender _sender;
	private final OutgoingEventHeader _header = new OutgoingEventHeader(0, 2);
	private SocketPackage _socketPackage;
	
	private final int _msgOffset = _header.getEventOffset();
	
	@Before
	public void setUp() {
		_buffer = new RingBuffer<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[BUFFER_SIZE];
			}
		}, 4);
		_sender = new NonblockingEventSender(_buffer, _buffer.newBarrier(), _header);
		_buffer.setGatingSequences(_sender.getSequence());
		
		_socketPackage = SocketPackage.create(_zmqSocket)
				.setSocketId(0);		
	}
	
	@Test
	public void sendWithSocketAndBufferReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		writeFakeMessage(outgoingBuffer);
		_buffer.publish(seq);
		
		assertTrue(_sender.sendIfReady(_socketPackage));
		assertEquals(0, _sender.getSequence().get());
	}
	
	@Test
	public void sendMultipleWithSocketAndBufferReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(true);
		
		// write the events out
		for (int i = 0; i < 3; i++) {
			long seq = _buffer.next();
			byte[] outgoingBuffer = _buffer.get(seq);
			writeFakeMessage(outgoingBuffer, i);
			_buffer.publish(seq);
		}
		// send the events
		for (int i = 0; i < 3; i++) {
			assertTrue(_sender.sendIfReady(_socketPackage));
		}
		assertEquals(2, _sender.getSequence().get());
	}
	
	@Test
	public void sendWithSocketNotReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(false);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		writeFakeMessage(outgoingBuffer);
		_buffer.publish(seq);
		
		assertFalse(_sender.sendIfReady(_socketPackage));
		assertEquals(-1, _sender.getSequence().get());
	}
	
	@Test
	public void sendWithSocketNotReadyThenReady() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(false)
			.thenReturn(true)
			.thenReturn(true);
	
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		writeFakeMessage(outgoingBuffer);
		_buffer.publish(seq);
		
		assertFalse(_sender.sendIfReady(_socketPackage));
		assertEquals(-1, _sender.getSequence().get());
		
		assertTrue(_sender.sendIfReady(_socketPackage));
		assertEquals(0, _sender.getSequence().get());
	}
	
	@Test
	public void sendWithNoPendingEvents() {	
		assertFalse(_sender.sendIfReady(_socketPackage));
		assertEquals(-1, _sender.getSequence().get());
		verifyZeroInteractions(_zmqSocket);
	}
	
	@Test
	public void sendWithNoPendingEventsThenPendingEvents() {				
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		assertFalse(_sender.sendIfReady(_socketPackage));
	
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		writeFakeMessage(outgoingBuffer);
		_buffer.publish(seq);
				
		assertTrue(_sender.sendIfReady(_socketPackage));
		assertEquals(0, _sender.getSequence().get());
	}
	
	@Test
	public void sendWithSocketNotReadyForSecondPart() {
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(false);
		
		long seq = _buffer.next();
		byte[] outgoingBuffer = _buffer.get(seq);
		writeFakeMessage(outgoingBuffer);
		_buffer.publish(seq);
		
		assertFalse(_sender.sendIfReady(_socketPackage));
		assertEquals(-1, _sender.getSequence().get());	
	}
	
	private void writeFakeMessage(byte[] outgoingBytes) {
		writeFakeMessage(outgoingBytes, 0);
	}
	
	private void writeFakeMessage(byte[] outgoingBytes, int seed) {
		byte[] segment1 = new byte[16];
		MessageBytesUtil.writeUUID(segment1, 0, UUID.fromString(String.format("abababab-abab-abab-%04d-abababababab", seed)));
		byte[] segment2 = new byte[256];
		for (int i = 0; i < 256; i += 4) {
			MessageBytesUtil.writeInt(segment2, i + _msgOffset + 16, (i * (seed + 1)) / 4);
		}
		TestEventSender.writeMessage(new byte[][] {segment1, segment2}, outgoingBytes, _header);
	}
	
}
