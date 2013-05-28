package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;

public interface EventQueueFactory {

	<T> EventQueue<T> createSingleProducerQueue(EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy);
	
	<T> EventQueue<T> createMultiProducerQueue(EventEntryHandler<T> eventEntryHandler, int size, WaitStrategy waitStrategy);
	
}
