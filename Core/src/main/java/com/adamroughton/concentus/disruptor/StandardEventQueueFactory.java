package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;

public class StandardEventQueueFactory implements EventQueueFactory {
	
	@Override
	public <T> EventQueue<T> createSingleProducerQueue(
			String name,
			EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy) {
		return new EventQueueImpl<>(new SingleProducerQueueStrategy<>(name, eventFactory, size, waitStrategy));
	}

	@Override
	public <T> EventQueue<T> createMultiProducerQueue(
			String name,
			EventEntryHandler<T> eventEntryHandler, int size,
			WaitStrategy waitStrategy) {
		return new EventQueueImpl<>(new MultiProducerQueueStrategy<>(name, eventEntryHandler, size, waitStrategy));
	}

}
