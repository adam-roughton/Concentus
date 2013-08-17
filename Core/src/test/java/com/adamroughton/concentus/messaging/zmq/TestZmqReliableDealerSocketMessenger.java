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

@RunWith(MockitoJUnitRunner.class)
public class TestZmqReliableDealerSocketMessenger {

	private static final int EVENT_BUFFER_LENGTH = 512;
	private final IncomingEventHeader _defaultHeader = new IncomingEventHeader(0, 2);
	
	@Mock private ZMQ.Socket _zmqSocket;
	private ZmqSocketMessenger _messenger;
	private ArrayBackedResizingBuffer _buffer;
	private DrivableClock _clock;
	
	@Before
	public void setUp() {
		_buffer = new ArrayBackedResizingBuffer(EVENT_BUFFER_LENGTH);
		_clock = new DrivableClock();
		_clock.setTime(5500000, TimeUnit.MILLISECONDS);
		_messenger = new ZmqReliableDealerSocketMessenger(0, "", _zmqSocket, _clock);
	}
	
	@Test
	public void recvSingleSegment() {
		IncomingEventHeader header = new IncomingEventHeader(0, 1);
		
		byte[] content = genContent(256);
		byte[] headerFrame = makeMsgHeader(-1, content);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(headerFrame))
			.then(fakeRecv(content));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, header, false));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(1, segments.length);
		assertArrayEquals(content, segments[0]);
	}
	
	@Test
	public void recvMultipleSegments() {
		IncomingEventHeader header = new IncomingEventHeader(0, 4);
		
		byte[][] contentSegments = new byte[3][];
		for (int i = 0; i < 3; i++) {
			contentSegments[i] = genContent(32, i);
		}
		byte[] headerFrame = makeMsgHeader(-1, contentSegments);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(headerFrame))
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
		
		for (int i = 0; i < 3; i++) {
			assertArrayEquals(contentSegments[i], segments[i]);
		}
	}
	
	@Test
	public void recvNoMessagesReady() {
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(-1);
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(false);
		
		assertFalse(_messenger.recv(_buffer, _defaultHeader, false));
		
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
		byte[] headerFrame = makeMsgHeader(-1, 32);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(headerFrame))
			.thenThrow(new ZMQException("Random error", 4));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		assertFalse(_defaultHeader.isValid(_buffer));
	}
	
	@Test
	public void recvLargeMessage() {
		IncomingEventHeader header = new IncomingEventHeader(0, 2);
		
		byte[][] contents = new byte[2][];
		genSegmentData(EVENT_BUFFER_LENGTH, 46, contents, 0, 1);
		byte[] headerFrame = makeMsgHeader(-1, contents);
		
		when(_zmqSocket.recv(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.then(fakeRecv(headerFrame))
			.then(fakeRecv(contents[0]))
			.then(fakeRecv(contents[1]));
		when(_zmqSocket.hasReceiveMore())
			.thenReturn(true)
			.thenReturn(true)
			.thenReturn(false);
		
		assertTrue(_messenger.recv(_buffer, _defaultHeader, false));
		
		verify(_zmqSocket, times(3)).recv(any(byte[].class), anyInt(), anyInt(), anyInt());
		assertTrue(_defaultHeader.isValid(_buffer));
		
		byte[][] segments = readMessageParts(_buffer, header);
		assertEquals(2, segments.length);
		
		for (int i = 0; i < 2; i++) {
			assertArrayEquals(contents[i], segments[i]);
		}	
	}
	
	@Test
	public void sendSingleSegment() {
		OutgoingEventHeader header = new OutgoingEventHeader(0, 1);
		
		when(_zmqSocket.send(any(byte[].class), anyInt(), anyInt(), anyInt()))
			.thenReturn(true)
			.thenReturn(true);
		
		int segmentLength = 256;
		byte[] content = genContent(256);
		
		int[] expectedOffsets = writeMessage(new byte[][] {content}, _buffer, header);		
		assertTrue(_messenger.send(_buffer, header, false));
		
		ByteArrayCaptor eBytesCaptor = new ByteArrayCaptor();
		ArgumentCaptor<Integer> eOffsetCaptor = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<Integer> eLengthCaptor = ArgumentCaptor.forClass(Integer.class);
		verify(_zmqSocket, times(2)).send(argThat(eBytesCaptor), eOffsetCaptor.capture(), eLengthCaptor.capture(), anyInt());
		
		List<byte[]> eventParts = eBytesCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
		
		assertEquals(2, eventParts.size());
		
		// seq frame
		byte[] expectedHeader = makeMsgHeader(-1, content);
		assertArrayEquals(expectedHeader, eventParts.get(0));
		
		// contents
		byte[] messageBytes = eventParts.get(1);
		int actualOffset = offsets.get(1);
		assertEquals(expectedOffsets[0], actualOffset);
		int actualLength = (int) lengths.get(1);
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
		
		
		byte[][] contents = new byte[3][];
		genSegmentData(32, 4, contents, 0, 1, 2);
		
		int[] expectedOffsets = writeMessage(contents, _buffer, header);
		assertTrue(_messenger.send(_buffer, header, false));
		
		verify(_zmqSocket, times(3)).send(any(byte[].class), anyInt(), anyInt(), intThat(hasFlags(ZMQ.SNDMORE)));
		verify(_zmqSocket).send(any(byte[].class), anyInt(), anyInt(), intThat(doesNotHaveFlags(ZMQ.SNDMORE)));
		
		List<byte[]> eventParts = eventPartCaptor.getAllValues();
		List<Integer> offsets = eOffsetCaptor.getAllValues();
		List<Integer> lengths = eLengthCaptor.getAllValues();
		assertEquals(4, eventParts.size());
		
		// seq frame
		byte[] expectedHeader = makeMsgHeader(-1, contents);
		assertArrayEquals(expectedHeader, eventParts.get(0));
		
		for (int i = 1; i < 3; i++) {
			int actualLength = (int)lengths.get(i);
			assertEquals(contents[i - 1].length, actualLength);
			int actualOffset = (int)offsets.get(i);
			assertEquals(expectedOffsets[i - 1], actualOffset);
			byte[] contentBytes = eventParts.get(i);
			assertRangeEqual(contents[i - 1], contentBytes, actualOffset, actualLength);
		}
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

	
}
