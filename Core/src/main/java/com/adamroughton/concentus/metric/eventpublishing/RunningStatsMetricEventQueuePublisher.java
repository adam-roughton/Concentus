package com.adamroughton.concentus.metric.eventpublishing;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.metric.MetricPublisher;
import com.adamroughton.concentus.metric.MetricType;
import com.adamroughton.concentus.util.RunningStats;

final class RunningStatsMetricEventQueuePublisher<TBuffer extends ResizingBuffer> 
		extends MetricEventQueuePublisherBase<TBuffer> implements MetricPublisher<RunningStats> {

	public RunningStatsMetricEventQueuePublisher(int metricSourceId, String metricName, MetricType metricType,
			EventQueue<TBuffer> pubQueue, OutgoingEventHeader pubEventHeader) {
		super(metricSourceId, metricName, metricType, pubQueue, pubEventHeader);
	}

	@Override
	public void publish(long bucketId, long bucketDuration, MetricMetaData metricMetaData,
			final RunningStats metricValue) {
		publishEvent(bucketId, bucketDuration, metricMetaData, new MetricValueWriterDelegate() {
			
			@Override
			public void write(ResizingBuffer buffer) {
				buffer.writeRunningStats(0, metricValue);
			}
		});
	}

}
