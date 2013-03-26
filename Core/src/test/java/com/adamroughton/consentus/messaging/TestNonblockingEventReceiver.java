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

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.consentus.messaging.NonblockingEventReceiver;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.adamroughton.consentus.messaging.ZmqTestUtil.*;

@RunWith(MockitoJUnitRunner.class)
public class TestNonblockingEventReceiver {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final IncomingEventHeader _header = new IncomingEventHeader(0, 2);
	
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
		_receiver = new NonblockingEventReceiver(_buffer, _header);
		
		_socketPackage = SocketPackage.create(_zmqSocket)
				.setSocketId(0);
	}
	
	@Test
	public void recvWithAvailableSpace() {
		_gatingSeq.set(0);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(new byte[16]))
			.then(fakeRecv(new byte[256]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_receiver.recvIfReady(_socketPackage));
		assertEquals(4, _buffer.getCursor());
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
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertFalse(_receiver.recvIfReady(_socketPackage));
		
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
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(new byte[256]))
			.then(fakeRecv(new byte[12]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_receiver.recvIfReady(_socketPackage));
		assertEquals(4, _buffer.getCursor());
	}
	
	@Test
	public void recvFailure() {
		_gatingSeq.set(0);
		
		when(_zmqSocket.recv(argThat(matchesLength(new byte[EVENT_BUFFER_LENGTH])), anyInt(), anyInt(), anyInt()))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertTrue(_receiver.recvIfReady(_socketPackage));
		assertEquals(4, _buffer.getCursor());
	}

}
