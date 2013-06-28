package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;

public final class SingleProducerQueueStrategy<T> extends EventQueueStrategyBase<T> {

	public SingleProducerQueueStrategy(String queueName, EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy) {
		super(queueName, RingBuffer.createSingleProducer(eventFactory, size, waitStrategy));
	}

	@Override
	public EventQueuePublisher<T> newQueuePublisher(String publisherName, boolean isBlocking) {
		return new SingleProducerEventQueuePublisher<>(publisherName, _ringBuffer, isBlocking);
	}
}
