package com.adamroughton.concentus.metric;

import java.util.Objects;

import com.adamroughton.concentus.Clock;

public final class MetricBucketInfo {

	private final Clock _clock;
	private final long _bucketDuration;
	
	public MetricBucketInfo(Clock clock, long bucketDuration) {
		_clock = Objects.requireNonNull(clock);
		_bucketDuration = bucketDuration;
	}
	
	public long getCurrentBucketId() {
		return _clock.currentMillis() / getBucketDuration();
	}
	
	public long getBucketStartTime(long metricBucketId) {
		return metricBucketId * getBucketDuration();
	}
	
	public long getBucketEndTime(long metricBucketId) {
		return (metricBucketId + 1) * getBucketDuration();
	}
	
	public long getBucketDuration() {
		return _bucketDuration;
	}
	
}
