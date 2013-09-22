package com.adamroughton.concentus.metric;

import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;

public interface MetricPublisher<TValue> {

	void publish(long bucketId, long bucketDuration, MetricMetaData metricMetaData, TValue metricValue);
	
}
