package com.adamroughton.concentus.messaging;

import java.util.Objects;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;

public final class MultiProducerEventQueuePublisher<T> implements EventQueuePublisher<T> {

	private final RingBuffer<T> _ringBuffer;
	private final EventEntryHandler<T> _entryHandler;
	private final boolean _isBlocking;
	private final T _tmpBuffer;
	private boolean _isPending;
	
	public MultiProducerEventQueuePublisher(RingBuffer<T> ringBuffer, EventEntryHandler<T> entryHandler, boolean isBlocking) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_entryHandler = Objects.requireNonNull(entryHandler);
		_tmpBuffer = entryHandler.newInstance();
		_isPending = false;
		_isBlocking = isBlocking;
	}

	@Override
	public T next() {
		if (!_isPending || publish()) {
			return _tmpBuffer;
		} else {
			return null;
		}
	}

	@Override
	public boolean publish() {
		long seq;
		if (_isBlocking) {
			seq = _ringBuffer.next();
		} else {
			try {
				seq = _ringBuffer.tryNext(1);
			} catch (InsufficientCapacityException eNoCapacity) {
				return false;
			}
		}
		T queueBuffer = _ringBuffer.get(seq);
		_entryHandler.copy(_tmpBuffer, queueBuffer);
		
		_entryHandler.clear(_tmpBuffer);
		_isPending = false;
		
		return true;
	}

	@Override
	public boolean hasUnpublished() {
		return _isPending;
	}
	
}