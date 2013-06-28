package com.adamroughton.concentus.metric;

public interface MetricCollector {

	StatsMetric createStatsMetric(String owner, String name, boolean isCumulative);
	
	CountMetric createThroughputMetric(String owner, String name, boolean isCumulative);
	
	CountMetric createCountMetric(String owner, String name, boolean isCumulative);
	
	/**
	 * Pushes any completed metric samples into the
	 * underlying sink.
	 */
	void flush();
	
	long nextDeadline();
	
}
