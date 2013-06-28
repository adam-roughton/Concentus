package com.adamroughton.concentus.metric;

public interface LongValueMetricPublisher extends MetricPublisher<Long> {

	void publishDirect(long bucketId, MetricMetaData metricMetaData, long metricValue);
	
}
