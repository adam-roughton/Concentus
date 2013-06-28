package com.adamroughton.concentus.metric.eventpublishing;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.metric.MetricMetaData;
import com.adamroughton.concentus.metric.MetricPublisher;
import com.adamroughton.concentus.metric.MetricType;
import com.adamroughton.concentus.util.RunningStats;

final class RunningStatsMetricEventQueuePublisher extends MetricEventQueuePublisherBase implements MetricPublisher<RunningStats> {

	public RunningStatsMetricEventQueuePublisher(String metricName, MetricType metricType,
			EventQueue<byte[]> pubQueue, OutgoingEventHeader pubEventHeader) {
		super(metricName, metricType, pubQueue, pubEventHeader);
	}

	@Override
	public void publish(long bucketId, MetricMetaData metricMetaData,
			final RunningStats metricValue) {
		publishEvent(bucketId, metricMetaData, new MetricValueWriterDelegate() {
			
			@Override
			public int write(byte[] buffer, int offset) {
				return MessageBytesUtil.writeRunningStats(buffer, offset, metricValue);
			}
		});
	}

}
