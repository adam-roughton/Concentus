package com.adamroughton.concentus.messaging;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

public class SingleProducerEventQueue<T> extends EventQueueBase<T> {

	public SingleProducerEventQueue(EventFactory<T> eventFactory, int queueSize, WaitStrategy waitStrategy) {
		super(new RingBuffer<>(eventFactory, new SingleThreadedClaimStrategy(queueSize), waitStrategy));
	}
	
	@Override
	public boolean isShared() {
		return false;
	}

	@Override
	protected EventQueuePublisher<T> doCreatePublisher(
			RingBuffer<T> ringBuffer, boolean isBlocking) {
		return new SingleProducerEventQueuePublisher<>(ringBuffer, isBlocking);
	}

}
