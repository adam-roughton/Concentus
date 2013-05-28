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

public final class EventQueueReaderImpl<T> implements EventQueueReader<T> {

	private final RingBuffer<T> _ringBuffer;
	private final boolean _isBlocking;
	private final SequenceBarrier _barrier;
	private final Sequence _sequence;
	private long _availableSeq = -1;
	
	public EventQueueReaderImpl(RingBuffer<T> ringBuffer, SequenceBarrier barrier, boolean isBlocking) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_isBlocking = isBlocking;
		_barrier = Objects.requireNonNull(barrier);
		_sequence = new Sequence(-1);
	}
	
	@Override
	public T get() throws AlertException, InterruptedException {
		long nextSeq = _sequence.get() + 1;
		
		if (nextSeq <= _availableSeq) {
			return _ringBuffer.get(nextSeq);
		} else if (_isBlocking) {
			return blockingGet(nextSeq);
		} else {
			return nonBlockingGet(nextSeq);
		}
	}
	
	private T blockingGet(long nextSeq) throws InterruptedException, AlertException {
		while (nextSeq > _availableSeq) {
			try {
				_availableSeq = _barrier.waitFor(nextSeq);
			} catch (TimeoutException e) {
				// ignore
			}
		}
		return _ringBuffer.get(nextSeq);
	}
	
	private T nonBlockingGet(long nextSeq) throws AlertException {
		_barrier.checkAlert();
		_availableSeq = _barrier.getCursor();
		if (nextSeq <= _availableSeq) {
			return _ringBuffer.get(nextSeq);
		} else {
			return null;
		}
	}

	@Override
	public void advance() {
		_sequence.incrementAndGet();
	}

	@Override
	public Sequence getSequence() {
		return _sequence;
	}

	@Override
	public SequenceBarrier getBarrier() {
		return _barrier;
	}

}
