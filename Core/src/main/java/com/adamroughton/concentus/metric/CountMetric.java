package com.adamroughton.concentus.metric;

import com.adamroughton.concentus.util.SlidingWindowLongMap;
import com.adamroughton.concentus.util.Util;

public final class CountMetric extends Metric<Long> {

	private final LongValueMetricPublisher _metricPublisher;
	private final SlidingWindowLongMap _countBuckets;
	private final LongMetricAccumulator _accumulator;
	
	public CountMetric(MetricMetaData metricMetaData, MetricBucketInfo metricBucketInfo, LongValueMetricPublisher metricPublisher, int bucketCount) {
		this(metricMetaData, metricBucketInfo, metricPublisher, 
				new SlidingWindowLongMap(Util.nextPowerOf2(bucketCount)), createAccumulator(metricMetaData.isCumulative()));
	}
	
	private CountMetric(MetricMetaData metricMetaData, MetricBucketInfo metricBucketInfo, LongValueMetricPublisher metricPublisher, 
			SlidingWindowLongMap countBuckets, LongMetricAccumulator accumulator) {
		super(metricMetaData, metricBucketInfo, metricPublisher, countBuckets, accumulator);
		_metricPublisher = metricPublisher;
		_countBuckets = countBuckets;
		_accumulator = accumulator;
	}
	
	public final void push(long count) {
		long bucketId = getMetricBucketInfo().getCurrentBucketId();
		long currentCount = 0;
		if (_countBuckets.containsIndex(bucketId)) {
			currentCount = _countBuckets.get(bucketId);
		}
		currentCount += count;
		_countBuckets.put(bucketId, currentCount);
	}

	@Override
	protected void doPublish(long bucketId, MetricMetaData metricMetaData) {
		long currentCount;
		if (_countBuckets.containsIndex(bucketId)) {
			currentCount = _countBuckets.getDirect(bucketId);
		} else {
			currentCount = 0;
		}
		_metricPublisher.publishDirect(bucketId, metricMetaData, _accumulator.getCumulativeValueDirect(currentCount));
	}
	
	private interface LongMetricAccumulator extends MetricAccumulator<Long> {
		long getCumulativeValueDirect(long newSample);
	}
	
	private static LongMetricAccumulator createAccumulator(boolean isCumulative) {
		if (isCumulative) {
			return new LongMetricAccumulator() {
				
				private long _accumulatedValue = 0;
				
				@Override
				public Long getCumulativeValue(Long newSample) {
					return getCumulativeValueDirect(newSample);
				}
				
				@Override
				public long getCumulativeValueDirect(long newSample) {
					_accumulatedValue += newSample;
					return _accumulatedValue;
				}
			};
		} else {
			return new LongMetricAccumulator() {

				@Override
				public Long getCumulativeValue(Long newSample) {
					return newSample;
				}
				
				@Override
				public long getCumulativeValueDirect(long newSample) {
					return newSample;
				}
			};
		}
	}

	@Override
	protected Long getDefaultValue() {
		return 0l;
	}
	
}
