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

import java.util.concurrent.CyclicBarrier;

import org.junit.*;

import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import static org.junit.Assert.*;

public class TestMultiProducerEventQueuePublisher {

	private RingBuffer<byte[]> _ringBuffer;
	private Sequence _gatingSeq = new Sequence(-1);
	private MultiProducerEventQueuePublisher<byte[]> _nonBlockingPublisher;
	private MultiProducerEventQueuePublisher<byte[]> _blockingPublisher;
	
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
		_nonBlockingPublisher = new MultiProducerEventQueuePublisher<>("", _ringBuffer, new EventEntryHandler<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[512];
			}

			@Override
			public void clear(byte[] event) {
				MessageBytesUtil.clear(event, 0, event.length);
			}

			@Override
			public void copy(byte[] source, byte[] destination) {
				System.arraycopy(source, 0, destination, 0, source.length);
			}
		} , false);
		
		_blockingPublisher = new MultiProducerEventQueuePublisher<>("", _ringBuffer, new EventEntryHandler<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[512];
			}

			@Override
			public void clear(byte[] event) {
				MessageBytesUtil.clear(event, 0, event.length);
			}

			@Override
			public void copy(byte[] source, byte[] destination) {
				System.arraycopy(source, 0, destination, 0, source.length);
			}
		} , true); 
	}
	
	@Test
	public void nonBlocking_publishWithAvailableSpace() {
		_gatingSeq.set(0);
		
		assertNotNull(_nonBlockingPublisher.next());
		_nonBlockingPublisher.publish();
		assertEquals(4, _ringBuffer.getCursor());
	}
	
	@Test
	public void nonBlocking_publishWithNoBufferSpace() {
		_gatingSeq.set(-1);
		assertNotNull(_nonBlockingPublisher.next());
		assertFalse(_nonBlockingPublisher.publish());
		assertEquals(3, _ringBuffer.getCursor());
	}
	
	@Test
	public void nonBlocking_publishWithNoBufferSpaceThenPublishWithSpace() {
		_gatingSeq.set(-1);
		
		byte[] buffer = _nonBlockingPublisher.next();
		assertNotNull(buffer);
		MessageBytesUtil.writeInt(buffer, 0, 15);
		assertFalse(_nonBlockingPublisher.publish());
		assertEquals(3, _ringBuffer.getCursor());
		
		_gatingSeq.set(3);
		
		buffer = _nonBlockingPublisher.next();
		assertNotNull(buffer);
		MessageBytesUtil.writeInt(buffer, 0, 24);
		_nonBlockingPublisher.publish();
		
		byte[] expected4th = new byte[512];
		MessageBytesUtil.writeInt(expected4th, 0, 15);
		
		byte[] expected5th = new byte[512];
		MessageBytesUtil.writeInt(expected5th, 0, 24);
		
		assertEquals(5, _ringBuffer.getCursor());
		assertArrayEquals(expected4th, _ringBuffer.get(4));
		assertArrayEquals(expected5th, _ringBuffer.get(5));
	}
	
	@Test
	public void nonBlocking_claimWithNoBufferSpaceAndPendingPublish() {
		_gatingSeq.set(-1);
		
		byte[] buffer = _nonBlockingPublisher.next();
		assertNotNull(buffer);
		MessageBytesUtil.writeInt(buffer, 0, 15);
		assertFalse(_nonBlockingPublisher.publish());
		assertEquals(3, _ringBuffer.getCursor());
		
		buffer = _nonBlockingPublisher.next();
		assertNull(buffer);
	}
	
	@Test
	public void nonBlocking_multiClaimWithNoBufferSpaceAndPendingPublish() {
		_gatingSeq.set(-1);
		
		byte[] buffer = _nonBlockingPublisher.next();
		assertNotNull(buffer);
		MessageBytesUtil.writeInt(buffer, 0, 15);
		assertFalse(_nonBlockingPublisher.publish());
		assertEquals(3, _ringBuffer.getCursor());
		
		buffer = _nonBlockingPublisher.next();
		assertNull(buffer);
		
		for (int i = 0; i < 5; i++) {
			buffer = _nonBlockingPublisher.next();
			assertNull(buffer);
			assertEquals(3, _ringBuffer.getCursor());
		}
		
		_gatingSeq.set(3);
		assertNotNull(_nonBlockingPublisher.next());
		
		byte[] expected = new byte[512];
		MessageBytesUtil.writeInt(expected, 0, 15);
		assertArrayEquals(expected, _ringBuffer.get(4));
	}
	
	@Test(timeout=5000)
	public void blocking_publishWithAvailableSpace() {
		_gatingSeq.set(0);
		
		assertNotNull(_blockingPublisher.next());
		_blockingPublisher.publish();
		assertEquals(4, _ringBuffer.getCursor());
	}
	
	@Test(timeout=5000)
	public void blocking_publishWithNoBufferSpaceThenPublishWithSpace() throws Exception {
		_gatingSeq.set(-1);
		
		final CyclicBarrier barrier = new CyclicBarrier(2);
		new Thread() {

			@Override
			public void run() {
				try {
					barrier.await();
					Thread.sleep(500);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				_gatingSeq.set(3);
			}
			
		}.start();
		
		byte[] buffer = _blockingPublisher.next();
		assertNotNull(buffer);
		MessageBytesUtil.writeInt(buffer, 0, 15);
		barrier.await();
		assertTrue(_blockingPublisher.publish());
		
		buffer = _blockingPublisher.next();
		assertNotNull(buffer);
		MessageBytesUtil.writeInt(buffer, 0, 24);
		_blockingPublisher.publish();
		
		byte[] expected4th = new byte[512];
		MessageBytesUtil.writeInt(expected4th, 0, 15);
		
		byte[] expected5th = new byte[512];
		MessageBytesUtil.writeInt(expected5th, 0, 24);
		
		assertEquals(5, _ringBuffer.getCursor());
		assertArrayEquals(expected4th, _ringBuffer.get(4));
		assertArrayEquals(expected5th, _ringBuffer.get(5));
	}

}
