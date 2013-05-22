package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class EventQueueReaderImpl<T> implements EventQueueReader<T> {

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
		_barrier.checkAlert();
		
		if (nextSeq > _availableSeq) {
			if (_isBlocking) {
				_availableSeq = _barrier.waitFor(nextSeq);
			} else {
				_availableSeq = _barrier.getCursor();				
			}
		}
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
