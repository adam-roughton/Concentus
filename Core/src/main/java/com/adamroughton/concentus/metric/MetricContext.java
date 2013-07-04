package com.adamroughton.concentus.metric;

import com.adamroughton.concentus.Clock;

public interface MetricContext {
	
	StatsMetric newStatsMetric(String reference, String metricName, boolean isCumulative);
	
	CountMetric newCountMetric(String reference, String metricName, boolean isCumulative);
	
	CountMetric newThroughputMetric(String reference, String metricName, boolean isCumulative);
	
	MetricBucketInfo getMetricBucketInfo();
	
	Clock getClock();

	void start();
	
	void halt();
	
}
