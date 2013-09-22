package com.adamroughton.concentus.metric;

import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;

public interface LongValueMetricPublisher extends MetricPublisher<Long> {

	void publishDirect(long bucketId, long bucketDuration, MetricMetaData metricMetaData, long metricValue);
	
}
