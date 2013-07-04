package com.adamroughton.concentus.metric;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.util.RunningStats;

public class NullMetricContext implements MetricContext {

	private final MetricBucketInfo _bucketInfo;
	private final Clock _clock = new Clock() {

		@Override
		public long currentMillis() {
			return 0;
		}

		@Override
		public long nanoTime() {
			return 0;
		}
		
	};
	
	public NullMetricContext() {
		_bucketInfo = new MetricBucketInfo(_clock, 1);
	}
	
	@Override
	public StatsMetric newStatsMetric(String reference, String metricName,
			boolean isCumulative) {
		return new StatsMetric(new MetricMetaData(0, reference, metricName, MetricType.STATS, isCumulative), _bucketInfo, 
				new MetricPublisher<RunningStats>() {
			
			@Override
			public void publish(long bucketId, MetricMetaData metricMetaData,
					RunningStats metricValue) {
			}
		}, 1);
	}

	@Override
	public CountMetric newCountMetric(String reference, String metricName,
			boolean isCumulative) {
		return nullCountMetric(MetricType.COUNT, reference, metricName, isCumulative);
	}

	@Override
	public CountMetric newThroughputMetric(String reference, String metricName,
			boolean isCumulative) {
		return nullCountMetric(MetricType.THROUGHPUT, reference, metricName, isCumulative);
	}
	
	private CountMetric nullCountMetric(MetricType type, String reference, String metricName,
			boolean isCumulative) {
		return new CountMetric(new MetricMetaData(0, reference, metricName, type, isCumulative), _bucketInfo, new LongValueMetricPublisher() {
			
			@Override
			public void publish(long bucketId, MetricMetaData metricMetaData,
					Long metricValue) {
			}
			
			@Override
			public void publishDirect(long bucketId, MetricMetaData metricMetaData,
					long metricValue) {
			}
		}, 1);
	}
	
	@Override
	public MetricBucketInfo getMetricBucketInfo() {
		return _bucketInfo;
	}

	@Override
	public void start() {
	}

	@Override
	public void halt() {
	}

	@Override
	public Clock getClock() {
		return _clock;
	}

}
