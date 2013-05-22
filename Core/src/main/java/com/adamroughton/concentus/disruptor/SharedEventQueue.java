package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;

public final class SharedEventQueue<T> extends EventQueueBase<T> {
	
	private final EventEntryHandler<T> _entryHandler;
	
	public SharedEventQueue(EventEntryHandler<T> entryHandler, ClaimStrategy claimStrategy, WaitStrategy waitStrategy) {
		super(new RingBuffer<>(entryHandler, claimStrategy, waitStrategy));
		_entryHandler = Objects.requireNonNull(entryHandler);
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
	
}
