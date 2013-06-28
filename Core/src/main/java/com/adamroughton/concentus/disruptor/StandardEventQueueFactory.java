package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.adamroughton.concentus.metric.MetricContext;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;

public class StandardEventQueueFactory implements EventQueueFactory {
	
	private final MetricContext _metricContext;
	
	public StandardEventQueueFactory(MetricContext metricContext) {
		_metricContext = Objects.requireNonNull(metricContext);
	}
	
	@Override
	public <T> EventQueue<T> createSingleProducerQueue(
			String name,
			EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy) {
		return new EventQueueImpl<>(new SingleProducerQueueStrategy<>(name, eventFactory, size, waitStrategy), _metricContext);
	}

	@Override
	public <T> EventQueue<T> createMultiProducerQueue(
			String name,
			EventEntryHandler<T> eventEntryHandler, int size,
			WaitStrategy waitStrategy) {
		return new EventQueueImpl<>(new MultiProducerQueueStrategy<>(name, eventEntryHandler, size, waitStrategy), _metricContext);
	}

}
