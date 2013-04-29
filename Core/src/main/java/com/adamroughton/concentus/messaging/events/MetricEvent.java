package com.adamroughton.concentus.messaging.events;

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public class MetricEvent extends ByteArrayBackedEvent {

	private static final int METRIC_TYPE_OFFSET = 0;
	private static final int SOURCE_ID_OFFSET = 4;
	private final static int METRIC_BUCKET_ID_OFFSET = 12;
	private final static int BUCKET_DURATION_OFFSET = 20;
	private static final int CONTENT_OFFSET = 28;
	
	protected MetricEvent(int metricType) {
		this(metricType, 0);
	}
	
	protected MetricEvent(int metricType, int defaultEventSize) {
		super(metricType, defaultEventSize + CONTENT_OFFSET);
	}

	public final int getMetricType() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(METRIC_TYPE_OFFSET));
	}
	
	public final long getSourceId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(SOURCE_ID_OFFSET));
	}
	
	public final void setSourceId(long sourceId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(SOURCE_ID_OFFSET), sourceId);
	}
	
	public final long getMetricBucketId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET));
	}
	
	public final void setMetricBucketId(long metricBucketId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET), metricBucketId);
	}

	public final long getBucketDuration() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(BUCKET_DURATION_OFFSET));
	}
	
	public final void setBucketDuration(long durationInMs) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(BUCKET_DURATION_OFFSET), durationInMs);
	}
	
	protected final int getContentOffset(int fieldOffset) {
		return getOffset(CONTENT_OFFSET) + fieldOffset;
	}
	
}
