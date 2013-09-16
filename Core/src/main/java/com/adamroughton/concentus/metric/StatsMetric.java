package com.adamroughton.concentus.metric;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.util.RunningStats;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;

public final class StatsMetric extends Metric<RunningStats> {
	
	private final StructuredSlidingWindowMap<RunningStats> _statsBuckets;
	
	public StatsMetric(MetricMetaData metricMetaData, MetricBucketInfo metricBucketInfo, 
			MetricPublisher<RunningStats> metricPublisher, int bucketCount) {
		this(metricMetaData,
			metricBucketInfo, 
			metricPublisher, 
			new StructuredSlidingWindowMap<>(bucketCount, RunningStats.class, 
				new ComponentFactory<RunningStats>() {

					@Override
					public RunningStats newInstance(Object[] initArgs) {
						return new RunningStats();
					}
				}, 
				new InitialiseDelegate<RunningStats>() {
		
					@Override
					public void initialise(RunningStats content) {
						content.reset();
					}
				})
		);
	}
	
	private StatsMetric(final MetricMetaData metricMetaData, MetricBucketInfo metricBucketInfo, 
			MetricPublisher<RunningStats> metricPublisher, StructuredSlidingWindowMap<RunningStats> statsBuckets) {
		super(metricMetaData, metricBucketInfo, metricPublisher, statsBuckets, createAccumulator(metricMetaData.isCumulative()));
		_statsBuckets = statsBuckets;
	}
	
	private static MetricAccumulator<RunningStats> createAccumulator(boolean isCumulative) {
		if (isCumulative) {
			return new MetricAccumulator<RunningStats>() {

				private final RunningStats _accumulatedStats = new RunningStats();
				
				@Override
				public RunningStats getCumulativeValue(RunningStats newSample) {
					_accumulatedStats.merge(newSample);
					return _accumulatedStats;
				}
			};
		} else {
			return new MetricAccumulator<RunningStats>() {

				@Override
				public RunningStats getCumulativeValue(RunningStats newSample) {
					return newSample;
				}
			};
		}
	}

	public void push(long sampleValue) {
		push((double) sampleValue);
	}

	public void push(double sampleValue) {
		long bucketId = getMetricBucketInfo().getCurrentBucketId();
		if (bucketId > _statsBuckets.getHeadIndex()) {
			_statsBuckets.advanceTo(bucketId);
		}
		if (_statsBuckets.containsIndex(bucketId)) {
			_statsBuckets.get(bucketId).push(sampleValue);
		}
	}

	@Override
	protected RunningStats getDefaultValue() {
		return new RunningStats();
	}
	
}
