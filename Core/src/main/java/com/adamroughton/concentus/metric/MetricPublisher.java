package com.adamroughton.concentus.metric;

public interface MetricPublisher<TValue> {

	void publish(long bucketId, MetricMetaData metricMetaData, TValue metricValue);
	
}
