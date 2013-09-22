package com.adamroughton.concentus.metric.eventpublishing;

import java.util.Objects;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.data.events.bufferbacked.MetricEvent;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.metric.MetricType;

abstract class MetricEventQueuePublisherBase<TBuffer extends ResizingBuffer> {

	private final MetricEvent _metricEvent = new MetricEvent();
	private final int _metricSourceId;
	private final MetricType _metricType;
	private final EventQueuePublisher<TBuffer> _eventPublisher;
	private final OutgoingEventHeader _pubEventHeader;
	
	public MetricEventQueuePublisherBase(
			int metricSourceId,
			String metricName, 
			MetricType metricType,
			EventQueue<TBuffer> pubQueue, 
			OutgoingEventHeader pubEventHeader) {
		_metricSourceId = metricSourceId;
		_metricType = Objects.requireNonNull(metricType);
		_eventPublisher = pubQueue.createPublisher(metricName, true);
		_pubEventHeader = Objects.requireNonNull(pubEventHeader);
	}
	
	protected interface MetricValueWriterDelegate {
		void write(ResizingBuffer buffer);
	}
	
	protected final void publishEvent(final long bucketId, final long bucketDuration, 
			final MetricMetaData metricMetaData, final MetricValueWriterDelegate writerDelegate) {
		final ResizingBuffer buffer = _eventPublisher.next();
		PubSubPattern.writePubEvent(buffer, _pubEventHeader, _metricEvent, new EventWriter<OutgoingEventHeader, MetricEvent>() {

			@Override
			public void write(OutgoingEventHeader header, MetricEvent event)
					throws Exception {
				event.setSourceId(_metricSourceId);
				event.setMetricId(metricMetaData.getMetricId());
				event.setMetricType(_metricType);
				event.setMetricBucketId(bucketId);
				event.setBucketDuration(bucketDuration);
				writerDelegate.write(event.getMetricValueSlice());
			}
		});
		_eventPublisher.publish();
	}
	
}
