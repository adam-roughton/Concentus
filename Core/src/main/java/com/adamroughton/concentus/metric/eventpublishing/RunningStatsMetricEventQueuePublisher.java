package com.adamroughton.concentus.metric.eventpublishing;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.metric.MetricMetaData;
import com.adamroughton.concentus.metric.MetricPublisher;
import com.adamroughton.concentus.metric.MetricType;
import com.adamroughton.concentus.util.RunningStats;

final class RunningStatsMetricEventQueuePublisher<TBuffer extends ResizingBuffer> 
		extends MetricEventQueuePublisherBase<TBuffer> implements MetricPublisher<RunningStats> {

	public RunningStatsMetricEventQueuePublisher(String metricName, MetricType metricType,
			EventQueue<TBuffer> pubQueue, OutgoingEventHeader pubEventHeader) {
		super(metricName, metricType, pubQueue, pubEventHeader);
	}

	@Override
	public void publish(long bucketId, MetricMetaData metricMetaData,
			final RunningStats metricValue) {
		publishEvent(bucketId, metricMetaData, new MetricValueWriterDelegate() {
			
			@Override
			public void write(ResizingBuffer buffer) {
				buffer.writeRunningStats(0, metricValue);
			}
		});
	}

}
