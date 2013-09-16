package com.adamroughton.concentus.cluster.coordinator;

import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;

public interface MetricRegistrationCallback {

	void newMetric(MetricMetaData metricMetaData);
	
}
