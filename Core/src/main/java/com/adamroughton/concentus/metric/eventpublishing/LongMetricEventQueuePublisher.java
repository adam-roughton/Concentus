package com.adamroughton.concentus.metric.eventpublishing;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.metric.LongValueMetricPublisher;
import com.adamroughton.concentus.metric.MetricType;

final class LongMetricEventQueuePublisher<TBuffer extends ResizingBuffer> 
		extends MetricEventQueuePublisherBase<TBuffer> implements LongValueMetricPublisher {

	public LongMetricEventQueuePublisher(int metricSourceId, String metricName, MetricType metricType,
			EventQueue<TBuffer> pubQueue, OutgoingEventHeader pubEventHeader) {
		super(metricSourceId, metricName, metricType, pubQueue, pubEventHeader);
	}

	@Override
	public void publish(long bucketId, long bucketDuration, MetricMetaData metricMetaData,
			Long metricValue) {
		publishDirect(bucketId, bucketDuration, metricMetaData, metricValue);
	}

	@Override
	public void publishDirect(long bucketId, long bucketDuration, MetricMetaData metricMetaData,
			final long metricValue) {
		publishEvent(bucketId, bucketDuration, metricMetaData, new MetricValueWriterDelegate() {
			
			@Override
			public void write(ResizingBuffer buffer) {
				buffer.writeLong(0, metricValue);
			}
		});
	}

}
