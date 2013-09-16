package com.adamroughton.concentus.metric;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.util.RunningStats;
import com.adamroughton.concentus.util.Util;

public abstract class MetricContextBase implements MetricContext {

	private final int _bucketCount;
	private final Clock _clock;
	private final MetricBucketInfo _bucketInfo;
	private int _metricSourceId;
	
	private final AtomicInteger _nextMetricId = new AtomicInteger(0);
	private final AtomicBoolean _hasStarted = new AtomicBoolean(false);
	
	public MetricContextBase(
			long metricTickMillis,
			long metricBufferMillis,
			Clock clock) {
		_bucketCount = Util.nextPowerOf2((int) ((metricBufferMillis / metricTickMillis) + (metricBufferMillis % metricTickMillis != 0? 1 : 0)));
		_clock = Objects.requireNonNull(clock);
		_bucketInfo = new MetricBucketInfo(_clock, metricTickMillis);
	}
	
	@Override
	public final StatsMetric newStatsMetric(String reference, String metricName,
			boolean isCumulative) {
		MetricMetaData metaData = newMetric(MetricType.STATS, reference, metricName, isCumulative);
		StatsMetric statsMetric = new StatsMetric(metaData, 
				_bucketInfo, 
				newStatsMetricPublisher(metaData), 
				_bucketCount);
		return statsMetric;
	}

	protected abstract MetricPublisher<RunningStats> newStatsMetricPublisher(MetricMetaData metaData);

	@Override
	public final CountMetric newCountMetric(String reference, String metricName,
			boolean isCumulative) {
		MetricMetaData metaData = newMetric(MetricType.COUNT, reference, metricName, isCumulative);
		CountMetric countMetric = new CountMetric(metaData, 
				_bucketInfo, 
				newCountMetricPublisher(metaData),
				_bucketCount);
		return countMetric;
	}

	protected abstract LongValueMetricPublisher newCountMetricPublisher(MetricMetaData metaData);

	@Override
	public final CountMetric newThroughputMetric(String reference, String metricName,
			boolean isCumulative) {
		MetricMetaData metaData = newMetric(MetricType.THROUGHPUT, reference, metricName, isCumulative);
		CountMetric throughputMetric = new CountMetric(metaData, 
				_bucketInfo,
				newCountMetricPublisher(metaData),
				_bucketCount);
		return throughputMetric;
	}
	
	private MetricMetaData newMetric(MetricType metricType, String reference, String metricName, boolean isCumulative) {
		if (!_hasStarted.get()) {
			throw new IllegalStateException("Metrics cannot be created until the metric context has been started.");
		}
		int metricId = _nextMetricId.getAndIncrement();
		MetricMetaData metaData = new MetricMetaData(_metricSourceId, metricId, reference, metricName, metricType, isCumulative);
		onNewMetric(metaData);
		return metaData;
	}
	
	protected void onNewMetric(MetricMetaData metaData) {
	}

	@Override
	public final MetricBucketInfo getMetricBucketInfo() {
		return _bucketInfo;
	}

	@Override
	public final Clock getClock() {
		return _clock;
	}
	
	public void start(int metricSourceId) {
		if (!_hasStarted.compareAndSet(false, true))
			throw new IllegalStateException("The metric context has already been started.");
		_metricSourceId = metricSourceId;
	}
	
	public void stop() {
	}
	
	
}
