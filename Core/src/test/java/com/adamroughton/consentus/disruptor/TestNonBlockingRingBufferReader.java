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
package com.adamroughton.consentus.disruptor;

import org.junit.*;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;

import static org.junit.Assert.*;

public class TestNonBlockingRingBufferReader {

	private RingBuffer<byte[]> _ringBuffer;
	private NonBlockingRingBufferReader<byte[]> _reader;
	
	@Before
	public void setUp() {
		_ringBuffer = new RingBuffer<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[512];
			}
		}, 4);
		_reader = new NonBlockingRingBufferReader<>(_ringBuffer, _ringBuffer.newBarrier());
		_ringBuffer.setGatingSequences(_reader.getSequence());	
	}
	
	@Test
	public void readWithBufferReady() throws Exception {		
		long seq = _ringBuffer.next();
		_ringBuffer.publish(seq);
		
		assertNotNull(_reader.getIfReady());
		_reader.advance();

		assertEquals(0, _reader.getSequence().get());
	}
	
	@Test
	public void readMultipleWithBufferReady() throws Exception {
		// write the events out
		for (int i = 0; i < 3; i++) {
			long seq = _ringBuffer.next();
			_ringBuffer.publish(seq);
		}
		// send the events
		for (int i = 0; i < 3; i++) {
			assertNotNull(_reader.getIfReady());
			_reader.advance();
		}
		assertEquals(2, _reader.getSequence().get());
	}
	
	@Test
	public void readWithNoEvents() throws Exception {	
		assertNull(_reader.getIfReady());
		assertEquals(-1, _reader.getSequence().get());
	}
	
	@Test
	public void readWithNoEventsThenReadWithEvents() throws Exception {				
		assertNull(_reader.getIfReady());
	
		long seq = _ringBuffer.next();
		_ringBuffer.publish(seq);
				
		assertNotNull(_reader.getIfReady());
		_reader.advance();
		assertEquals(0, _reader.getSequence().get());
	}
	
}
