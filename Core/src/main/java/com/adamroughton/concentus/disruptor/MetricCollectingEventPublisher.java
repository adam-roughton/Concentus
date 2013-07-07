package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.metric.StatsMetric;
import com.adamroughton.concentus.util.Util;

final class MetricCollectingEventPublisher<T> implements EventQueuePublisher<T>, PrePublishDelegate {
	
	private final EnqueueTimeCollector _enqueueTimeCollector;
	private final EventQueueStrategy<T> _decoratedStrategy;
	private final Clock _clock;
	private final EventQueuePublisher<T> _decoratedPublisher;
	private PrePublishDelegate _additionalPrePubDelegate;
	
	private final MetricGroup _metrics;
	private final CountMetric _claimAttemptThroughputMetric;
	private final CountMetric _failedClaimThroughputMetric;
	private final CountMetric _publishAttemptThroughputMetric;
	private final CountMetric _failedPublishThroughputMetric;
	private final StatsMetric _capacityPercentageStatsMetric;
	
	private final long _queueLength;
	
	private long _nextPubTime;
	private final long _capacitySamplePeriod;
	private long _nextCapacitySampleTime;
	
	public MetricCollectingEventPublisher(
			MetricContext metricContext,
			EnqueueTimeCollector enqueueTimeCollector,
			EventQueueStrategy<T> decoratedStrategy,
			Clock clock,
			EventQueuePublisher<T> decoratedPublisher) {
		_enqueueTimeCollector = Objects.requireNonNull(enqueueTimeCollector);
		_decoratedStrategy = Objects.requireNonNull(decoratedStrategy);
		_queueLength = _decoratedStrategy.getLength();
		_clock = clock;
		_decoratedPublisher = Objects.requireNonNull(decoratedPublisher);
		_decoratedPublisher.setPrePublishDelegate(this);
		_additionalPrePubDelegate = new NullPrePublishDelegate();
		
		long capacitySamplePeriod = metricContext.getMetricBucketInfo().getBucketDuration() / 10;
		if (capacitySamplePeriod < 10) {
			capacitySamplePeriod = 10;
		}
		_capacitySamplePeriod = capacitySamplePeriod;
		
		_metrics = new MetricGroup();
		String reference = String.format("QueuePublisher(%1$s->%2$s)", _decoratedPublisher.getName(), _decoratedStrategy.getQueueName());
		_claimAttemptThroughputMetric = _metrics.add(metricContext.newThroughputMetric(reference, "claimAttemptThroughput", false));
		_failedClaimThroughputMetric = _metrics.add(metricContext.newThroughputMetric(reference, "failedClaimThroughput", false));
		_publishAttemptThroughputMetric = _metrics.add(metricContext.newThroughputMetric(reference, "publishAttemptThroughput", false));
		_failedPublishThroughputMetric = _metrics.add(metricContext.newThroughputMetric(reference, "failedPublishThroughput", false));
		_capacityPercentageStatsMetric = _metrics.add(metricContext.newStatsMetric(reference, "capacityPercentageStats", false));
	}
	
	@Override
	public void beforePublish(long sequence) {
		_additionalPrePubDelegate.beforePublish(sequence);
		_enqueueTimeCollector.setEnqueueTime(sequence, _clock.currentMillis());
	}

	@Override
	public T next() {
		_claimAttemptThroughputMetric.push(1);
		T claimedSlot = _decoratedPublisher.next();
		if (claimedSlot == null) {
			_failedClaimThroughputMetric.push(1);
		}
		emitMetricIfNeeded();
		return claimedSlot;
	}

	@Override
	public boolean publish() {
		_publishAttemptThroughputMetric.push(1);
		boolean didPublish = _decoratedPublisher.publish();
		if (!didPublish) {
			_failedPublishThroughputMetric.push(1);
		}
		emitMetricIfNeeded();
		return didPublish;
	}

	@Override
	public long getLastPublishedSequence() {
		return _decoratedPublisher.getLastPublishedSequence();
	}

	@Override
	public boolean hasUnpublished() {
		return _decoratedPublisher.hasUnpublished();
	}

	@Override
	public T getUnpublished() {
		return _decoratedPublisher.getUnpublished();
	}

	@Override
	public void setPrePublishDelegate(PrePublishDelegate delegate) {
		_additionalPrePubDelegate = Objects.requireNonNull(delegate);
	}

	@Override
	public String getName() {
		return _decoratedPublisher.getName();
	}

	private void emitMetricIfNeeded() {
		long now = _clock.currentMillis();
		if (now > _nextCapacitySampleTime) {
			_capacityPercentageStatsMetric.push(getCapacityPercentage());	
			_nextCapacitySampleTime = now + _capacitySamplePeriod;
		}
		if (now >= _nextPubTime) {
			_metrics.publishPending();
			_nextPubTime = _metrics.nextBucketReadyTime();
		}
	}
	
	private double getCapacityPercentage() {
		return 100 - Util.getPercentage(_decoratedStrategy.remainingCapacity(), _queueLength);
	}

}
