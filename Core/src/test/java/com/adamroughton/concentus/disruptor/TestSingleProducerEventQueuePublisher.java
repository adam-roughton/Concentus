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
package com.adamroughton.concentus.disruptor;

import org.junit.*;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.junit.Assert.*;

public class TestSingleProducerEventQueuePublisher {

	private RingBuffer<byte[]> _ringBuffer;
	private Sequence _gatingSeq = new Sequence(-1);
	private SingleProducerEventQueuePublisher<byte[]> _publisher;
	
	@Before
	public void setUp() {
		_ringBuffer = RingBuffer.createMultiProducer(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[512];
			}
		}, 4);
		_ringBuffer.addGatingSequences(_gatingSeq);
		
		// fake publish to get to wrap around point
		for (int i = 0; i < 4; i++) {
			long seq = _ringBuffer.next();
			_ringBuffer.publish(seq);
		}
		// gating seq set such that no buffer space is available
		_gatingSeq.set(-1);
		_publisher = new SingleProducerEventQueuePublisher<>(_ringBuffer, false);
	}
	
	@Test
	public void claimWithAvailableSpace() {
		_gatingSeq.set(0);
		
		assertNotNull(_publisher.next());
		_publisher.publish();
		assertEquals(4, _ringBuffer.getCursor());
	}
	
	@Test
	public void claimWithNoBufferSpace() {
		_gatingSeq.set(-1);
		assertNull(_publisher.next());		
		assertEquals(3, _ringBuffer.getCursor());
	}
	
	@Test
	public void claimWithNoBufferSpaceThenClaimWithSpace() {
		_gatingSeq.set(-1);
		
		assertNull(_publisher.next());
		assertEquals(3, _ringBuffer.getCursor());
		
		_gatingSeq.set(3);
		
		assertNotNull(_publisher.next());
		_publisher.publish();
		
		assertEquals(4, _ringBuffer.getCursor());
	}

}
