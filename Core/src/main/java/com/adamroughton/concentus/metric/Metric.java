package com.adamroughton.concentus.metric;

import java.util.Objects;

import com.adamroughton.concentus.util.SlidingWindowMap;

public abstract class Metric<T> {

	private final MetricMetaData _metricMetaData;
	private final MetricBucketInfo _metricBucketInfo;
	private final MetricPublisher<T> _metricPublisher;
	private final SlidingWindowMap<T> _windowMap;
	private final MetricAccumulator<T> _accumulator;
	
	private final int _windowLength;
	private long _lastPublishedBucketId;
	
	public Metric(
			MetricMetaData metricMetaData, 
			MetricBucketInfo metricBucketInfo,
			MetricPublisher<T> metricPublisher,
			SlidingWindowMap<T> windowMap, 
			MetricAccumulator<T> accumulator) {
		_metricMetaData = Objects.requireNonNull(metricMetaData);
		_metricBucketInfo = Objects.requireNonNull(metricBucketInfo);
		_accumulator = Objects.requireNonNull(accumulator);
		_metricPublisher = Objects.requireNonNull(metricPublisher);
		_windowMap = Objects.requireNonNull(windowMap);
		_windowLength = _windowMap.getLength();
		_lastPublishedBucketId = _metricBucketInfo.getCurrentBucketId() - 1;
	}
	
	/**
	 * Publishes all pending buckets.
	 */
	public final void publishPending() {
		long currentBucketId = _metricBucketInfo.getCurrentBucketId();
		for (long bucketId = bucketIdLowerBound(); bucketId < currentBucketId; bucketId++) {
			doPublish(bucketId, _metricMetaData);
			_windowMap.remove(bucketId);
			_lastPublishedBucketId = bucketId;
		}
	}
	
	/**
	 * Gets the lowest possible unpublished bucketId for the metric given buffer size constraints and the
	 * last published bucket ID.
	 * @return the lower bound bucket ID for the metric (i.e. the lowest possible pending bucket ID)
	 */
	public final long bucketIdLowerBound() {
		long lastPossibleWindowBucketId = _windowMap.getHeadIndex() - _windowLength + 1;
		return Math.max(_lastPublishedBucketId + 1, lastPossibleWindowBucketId);
	}
	
	public final long nextBucketReadyTime() {
		long nextPendingBucket = bucketIdLowerBound();
		return _metricBucketInfo.getBucketEndTime(nextPendingBucket);
	}
	
	public final long lastPublishedBucketId() {
		return _lastPublishedBucketId;
	}
	
	protected void doPublish(long bucketId, MetricMetaData metricMetaData) {
		T metricValue;
		if (_windowMap.containsIndex(bucketId)) {
			metricValue = _windowMap.get(bucketId);
		} else {
			metricValue = getDefaultValue();
		}		
		_metricPublisher.publish(bucketId, metricMetaData, 
				_accumulator.getCumulativeValue(metricValue));
	}
	
	protected abstract T getDefaultValue();
	
	public final MetricBucketInfo getMetricBucketInfo() {
		return _metricBucketInfo;
	}
	
	public final MetricMetaData getMetricMetaData() {
		return _metricMetaData;
	}
	
	protected interface MetricAccumulator<T> {
		T getCumulativeValue(T newSample);
	}
}
