package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;

public class StandardEventQueueFactory implements EventQueueFactory {

	public StandardEventQueueFactory() {
	}
	
	
	
	@Override
	public <T> EventQueue<T> createSingleProducerQueue(
			EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy) {
		return new SingleProducerEventQueue<>(eventFactory, size, waitStrategy);
	}

	@Override
	public <T> EventQueue<T> createMultiProducerQueue(
			EventEntryHandler<T> eventEntryHandler, int size,
			WaitStrategy waitStrategy) {
		return new MultiProducerEventQueue<>(eventEntryHandler, size, waitStrategy);
	}

}
