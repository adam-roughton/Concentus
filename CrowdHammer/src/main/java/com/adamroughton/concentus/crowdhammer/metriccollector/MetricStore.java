package com.adamroughton.concentus.crowdhammer.metriccollector;

import java.io.Closeable;
import java.util.Set;

import org.javatuples.Pair;

import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.util.RunningStats;

public interface MetricStore extends Closeable {

	void pushTestRunMetaData(int runId, String name, int clientCount, 
			long durationMillis, Class<? extends CollectiveApplication> applicationClass, 
			Class<? extends ClientAgent> agentClass, Set<Pair<String, Integer>> deploymentInfo);
	
	void pushSourceMetaData(int runId, int sourceId, String name, String serviceType);
	
	void pushMetricMetaData(int runId, int sourceId, int metricId, String reference, String metricName, boolean isCumulative);
	
	void pushStatsMetric(int runId, int sourceId, int metricId, long bucketId, long duration, RunningStats runningStats);
	
	void pushCountMetric(int runId, int sourceId, int metricId, long bucketId, long duration, long count);
	
	void pushThroughputMetric(int runId, int sourceId, int metricId, long bucketId, long duration, long count);
	
	void onEndOfBatch();
	
	boolean isClosed();
	
}
