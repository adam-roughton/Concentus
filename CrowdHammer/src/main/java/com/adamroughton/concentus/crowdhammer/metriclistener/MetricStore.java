package com.adamroughton.concentus.crowdhammer.metriclistener;

import com.adamroughton.concentus.util.RunningStats;

public interface MetricStore {

	void pushRunMetaData(int runId, int clientCount);
	
	void pushMetricMetaData(int sourceId, int metricId, String reference, String metricName);
	
	void pushStatsMetric(int runId, int sourceId, int metricId, long bucketId, long duration, RunningStats runningStats);
	
	void pushCountMetric(int runId, int sourceId, int metricId, long bucketId, long duration, long count);
	
	void pushThroughputMetric(int runId, int sourceId, int metricId, long bucketId, long duration, long count);
	
}
