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

import java.util.Objects;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;

public final class MultiProducerEventQueue<T> extends EventQueueBase<T> {
	
	private final EventEntryHandler<T> _entryHandler;
	
	public MultiProducerEventQueue(EventEntryHandler<T> eventEntryHandler, int size, WaitStrategy waitStrategy) {
		super(RingBuffer.createMultiProducer(eventEntryHandler, size, waitStrategy));
		_entryHandler = Objects.requireNonNull(eventEntryHandler);
	}

	@Override
	public boolean isShared() {
		return true;
	}

	@Override
	protected EventQueuePublisher<T> doCreatePublisher(
			RingBuffer<T> ringBuffer, boolean isBlocking) {
		return new MultiProducerEventQueuePublisher<>(ringBuffer, _entryHandler, isBlocking);
	}
	
	@Override
	public SequenceBarrier newBarrier(Sequence...sequencesToTrack) {
		final SequenceBarrier wrappedBarrier = _ringBuffer.newBarrier(sequencesToTrack);
		return new SequenceBarrier() {	
			
			/*
			 * We need to track the last returned cursor to work
			 * around the bug in MultiProducerSequencer where the
			 * cursor sequence is updated on next, before publishing.
			 * When a request for the latest cursor comes in, we scan
			 * ahead to find the highest cursor value since the last
			 * known one, ensuring no unpublished gaps are left as can
			 * happen if multiple publishers call next and the publisher
			 * with a higher claimed sequence publishes before those with
			 * lower claimed sequences. This functionality is actually present
			 * in MultiProducerSequencer (getHighestPublishedSequence) though
			 * the RingBuffer does not expose a constructor that we can use
			 * to substitute a fix.
			 */
			private long _lastCursor = -1;
			
			@Override
			public long waitFor(long sequence) throws AlertException,
					InterruptedException, TimeoutException {
				return wrappedBarrier.waitFor(sequence);
			}

			@Override
			public long getCursor() {
				/*
				 * The MultiProducerSequencer increments the cursor
				 * sequence before publishing. This causes the getCursor
				 * call on any sequence barrier from a MultiProducerSequencer
				 * backed RingBuffer to be racy. This hack ensures that
				 * the cursor is actually published before returning.
				 */
				long reportedCursor = wrappedBarrier.getCursor();
				long cursor = reportedCursor;
				for (long sequence = _lastCursor + 1; sequence <= reportedCursor; sequence++) {
					if (!_ringBuffer.isPublished(sequence)) {
						cursor = sequence - 1;
						break;
					}
				}
				_lastCursor = cursor;
				return cursor;
			}

			@Override
			public boolean isAlerted() {
				return wrappedBarrier.isAlerted();
			}

			@Override
			public void alert() {
				wrappedBarrier.alert();
			}

			@Override
			public void clearAlert() {
				wrappedBarrier.clearAlert();
			}

			@Override
			public void checkAlert() throws AlertException {
				wrappedBarrier.checkAlert();
			}
			
		};		
	}
	
}
