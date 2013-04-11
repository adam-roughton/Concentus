package com.adamroughton.concentus;

import uk.co.real_logic.intrinsics.ComponentFactory;

/**
 * Manages the collection of metrics such that changes to metrics are stored against 
 * the right metric bucket (given the current time).
 * @author Adam Roughton
 *
 * @param <T>
 */
public class MetricContainer<T> {

	private final StructuredSlidingWindowMap<T> _metricContainer;
	
	public MetricContainer(int bufferSize, ComponentFactory<T> metricEntryFactory) {
		if (bufferSize <= 0) 
			throw new IllegalArgumentException(String.format("The buffer size must be greater than 0 (was %d)", bufferSize));
		_metricContainer = new StructuredSlidingWindowMap<>(Util.nextPowerOf2(bufferSize), metricEntryFactory);
	}
	
	public T getMetricEntry() {
		long bucketId = getCurrentMetricBucketId();
		return getMetricEntry(bucketId);
	}
	
	public T getMetricEntry(long metricBucketId) {
		if (!_metricContainer.containsIndex(metricBucketId)) {
			_metricContainer.advanceTo(metricBucketId);
		}
		return _metricContainer.get(metricBucketId);
	}
	
	public void forEachPending(MetricLamda<T> lamda) {
		long windowStartIndex = _metricContainer.getHeadIndex() - _metricContainer.windowSize() + 1;
		for (long bucketIndex = windowStartIndex; bucketIndex <= _metricContainer.getHeadIndex(); bucketIndex++) {
			if (_metricContainer.containsIndex(bucketIndex)) {
				lamda.call(_metricContainer.get(bucketIndex));
				_metricContainer.remove(bucketIndex);
			}
		}
	}
	
	public interface MetricLamda<T> {
		void call(T metricEntry);
	}
	
	public static long getCurrentMetricBucketId() {
		return System.currentTimeMillis() / Constants.METRIC_TICK;
	}
	
	public static long getMetricBucketStartTime(long metricBucketId) {
		return metricBucketId * Constants.METRIC_TICK;
	}
	
	public static long getMetricBucketEndTime(long metricBucketId) {
		return (metricBucketId + 1) * Constants.METRIC_TICK;
	}
	
}
