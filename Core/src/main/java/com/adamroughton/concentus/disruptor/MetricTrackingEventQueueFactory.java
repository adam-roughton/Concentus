package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.metric.MetricContext;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;

public class MetricTrackingEventQueueFactory implements EventQueueFactory {
	
	private final MetricContext _metricContext;
	private final Clock _clock;
	
	public MetricTrackingEventQueueFactory(MetricContext metricContext, Clock clock) {
		_metricContext = Objects.requireNonNull(metricContext);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public <T> EventQueue<T> createSingleProducerQueue(
			String queueName,
			EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy) {
		return new EventQueueImpl<>(new QueueMetricStrategy<>(_metricContext, 
				new SingleProducerQueueStrategy<>(queueName, eventFactory, size, waitStrategy), _clock), _metricContext);
	}

	@Override
	public <T> EventQueue<T> createMultiProducerQueue(
			String queueName,
			EventEntryHandler<T> eventEntryHandler, int size,
			WaitStrategy waitStrategy) {
		return new EventQueueImpl<>(new QueueMetricStrategy<>(_metricContext, 
				new MultiProducerQueueStrategy<>(queueName, eventEntryHandler, size, waitStrategy), _clock), _metricContext);
	}

}
