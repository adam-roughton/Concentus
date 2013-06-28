package com.adamroughton.concentus.metric.eventpublishing;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.metric.LongValueMetricPublisher;
import com.adamroughton.concentus.metric.MetricMetaData;
import com.adamroughton.concentus.metric.MetricType;

final class LongMetricEventQueuePublisher extends MetricEventQueuePublisherBase implements LongValueMetricPublisher {

	public LongMetricEventQueuePublisher(String metricName, MetricType metricType,
			EventQueue<byte[]> pubQueue, OutgoingEventHeader pubEventHeader) {
		super(metricName, metricType, pubQueue, pubEventHeader);
	}

	@Override
	public void publish(long bucketId, MetricMetaData metricMetaData,
			Long metricValue) {
		publishDirect(bucketId, metricMetaData, metricValue);
	}

	@Override
	public void publishDirect(long bucketId, MetricMetaData metricMetaData,
			final long metricValue) {
		publishEvent(bucketId, metricMetaData, new MetricValueWriterDelegate() {
			
			@Override
			public int write(byte[] buffer, int offset) {
				MessageBytesUtil.writeLong(buffer, offset, metricValue);
				return 8;
			}
		});
	}

}
