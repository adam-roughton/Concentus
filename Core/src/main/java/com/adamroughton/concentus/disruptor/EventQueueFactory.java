package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;

public interface EventQueueFactory {

	<T> EventQueue<T> createSingleProducerQueue(String queueName, 
			EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy);
	
	<T> EventQueue<T> createMultiProducerQueue(String queueName, 
			EventEntryHandler<T> eventEntryHandler, int size, WaitStrategy waitStrategy);
	
}
