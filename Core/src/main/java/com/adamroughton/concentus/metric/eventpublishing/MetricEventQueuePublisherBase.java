package com.adamroughton.concentus.metric.eventpublishing;

import java.util.Objects;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.events.MetricEvent;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.metric.MetricMetaData;
import com.adamroughton.concentus.metric.MetricType;

abstract class MetricEventQueuePublisherBase<TBuffer extends ResizingBuffer> {

	private final MetricEvent _metricEvent = new MetricEvent();
	private final MetricType _metricType;
	private final EventQueuePublisher<TBuffer> _eventPublisher;
	private final OutgoingEventHeader _pubEventHeader;
	
	public MetricEventQueuePublisherBase(
			String metricName, 
			MetricType metricType,
			EventQueue<TBuffer> pubQueue, 
			OutgoingEventHeader pubEventHeader) {
		_metricType = Objects.requireNonNull(metricType);
		_eventPublisher = pubQueue.createPublisher(metricName, true);
		_pubEventHeader = Objects.requireNonNull(pubEventHeader);
	}
	
	protected interface MetricValueWriterDelegate {
		void write(ResizingBuffer buffer);
	}
	
	protected final void publishEvent(final long bucketId, final MetricMetaData metricMetaData, final MetricValueWriterDelegate writerDelegate) {
		final ResizingBuffer buffer = _eventPublisher.next();
		PubSubPattern.writePubEvent(buffer, _pubEventHeader, _metricEvent, new EventWriter<OutgoingEventHeader, MetricEvent>() {

			@Override
			public void write(OutgoingEventHeader header, MetricEvent event)
					throws Exception {
				event.setMetricId(metricMetaData.getMetricId());
				event.setMetricType(_metricType.getId());
				event.setMetricBucketId(bucketId);
				writerDelegate.write(event.getMetricValueSlice());
			}
		});
		_eventPublisher.publish();
	}
	
}
