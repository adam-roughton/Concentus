package com.adamroughton.concentus.crowdhammer.metriclistener;

import java.io.Closeable;
import java.util.UUID;

import com.adamroughton.concentus.util.RunningStats;

public interface MetricStore extends Closeable {

	void pushRunMetaData(int runId, int clientCount, long durationMillis);
	
	void pushSourceMetaData(UUID sourceId, String name);
	
	void pushMetricMetaData(int runId, UUID sourceId, int metricId, String reference, String metricName, boolean isCumulative);
	
	void pushStatsMetric(int runId, UUID sourceId, int metricId, long bucketId, long duration, RunningStats runningStats);
	
	void pushCountMetric(int runId, UUID sourceId, int metricId, long bucketId, long duration, long count);
	
	void pushThroughputMetric(int runId, UUID sourceId, int metricId, long bucketId, long duration, long count);
	
	void onEndOfBatch();
	
	boolean isClosed();
}
