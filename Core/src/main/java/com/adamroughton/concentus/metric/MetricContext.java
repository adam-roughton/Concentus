package com.adamroughton.concentus.metric;

public interface MetricContext {
	
	StatsMetric newStatsMetric(String reference, String metricName, boolean isCumulative);
	
	CountMetric newCountMetric(String reference, String metricName, boolean isCumulative);
	
	CountMetric newThroughputMetric(String reference, String metricName, boolean isCumulative);
	
	MetricBucketInfo getMetricBucketInfo();

	void start();
	
	void halt();
	
}
