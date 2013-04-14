package com.adamroughton.concentus;

import java.util.Objects;

import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.adamroughton.concentus.util.Util;

import uk.co.real_logic.intrinsics.ComponentFactory;
import static com.adamroughton.concentus.Constants.METRIC_TICK;

/**
 * Manages the collection of metrics such that changes to metrics are stored against 
 * the right metric bucket (given the current time).
 * @author Adam Roughton
 *
 * @param <T>
 */
public class MetricContainer<T> {

	private final Clock _clock;
	private final StructuredSlidingWindowMap<T> _windowMap;
	
	public MetricContainer(Clock clock, int bufferSize, ComponentFactory<T> metricEntryFactory, InitialiseDelegate<T> initialiseDelegate) {
		_clock = Objects.requireNonNull(clock);
		if (bufferSize <= 0) 
			throw new IllegalArgumentException(String.format("The buffer size must be greater than 0 (was %d)", bufferSize));
		_windowMap = new StructuredSlidingWindowMap<>(Util.nextPowerOf2(bufferSize), metricEntryFactory, initialiseDelegate);
	}
	
	public T getMetricEntry() {
		long bucketId = getCurrentMetricBucketId();
		return getMetricEntry(bucketId);
	}
	
	public T getMetricEntry(long metricBucketId) {
		if (!_windowMap.containsIndex(metricBucketId)) {
			_windowMap.advanceTo(metricBucketId);
		}
		return _windowMap.get(metricBucketId);
	}
	
	public void forEachPending(MetricLamda<T> lamda) {		
		long windowStartIndex = _windowMap.getHeadIndex() - _windowMap.windowSize() + 1;
		long currentBucketId = getCurrentMetricBucketId();
		for (long bucketId = windowStartIndex; bucketId <= _windowMap.getHeadIndex() && bucketId < currentBucketId; bucketId++) {
			if (_windowMap.containsIndex(bucketId)) {
				lamda.call(bucketId, _windowMap.get(bucketId));
				_windowMap.remove(bucketId);
			}
		}
	}
	
	public interface MetricLamda<T> {
		void call(long bucketId, T metricEntry);
	}
	
	public long getCurrentMetricBucketId() {
		return _clock.currentMillis() / METRIC_TICK;
	}
	
	public long getMetricBucketStartTime(long metricBucketId) {
		return metricBucketId * METRIC_TICK;
	}
	
	public long getMetricBucketEndTime(long metricBucketId) {
		return (metricBucketId + 1) * METRIC_TICK;
	}
	
	public long getBucketDuration() {
		return METRIC_TICK;
	}
	
	public interface ClearStateDelegate<T> {
		void clear(T metric);
	}
	
}
