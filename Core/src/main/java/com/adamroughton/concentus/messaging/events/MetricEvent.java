/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.messaging.events;

import java.util.UUID;

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public final class MetricEvent extends ByteArrayBackedEvent {

	private static final int SOURCE_ID_OFFSET = 0;
	private static final int METRIC_ID_OFFSET = 16;
	private static final int METRIC_TYPE_OFFSET = 20;
	private static final int METRIC_BUCKET_ID_OFFSET = 24;
	private static final int BUCKET_DURATION_OFFSET = 32;
	private static final int METRIC_VALUE_LENGTH_OFFSET = 40;
	private static final int METRIC_VALUE_OFFSET = 48;
	
	public MetricEvent() {
		super(EventType.METRIC.getId());
	}
	
	public UUID getSourceId() {
		return MessageBytesUtil.readUUID(getBackingArray(), getOffset(SOURCE_ID_OFFSET));
	}
	
	public void setSourceId(UUID sourceId) {
		MessageBytesUtil.writeUUID(getBackingArray(), getOffset(SOURCE_ID_OFFSET), sourceId);
	}
	
	public int getMetricId() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(METRIC_ID_OFFSET));
	}
	
	public void setMetricId(int metricId) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(METRIC_ID_OFFSET), metricId);
	}
	
	public int getMetricType() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(METRIC_TYPE_OFFSET));
	}
	
	public void setMetricType(int metricType) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(METRIC_TYPE_OFFSET), metricType);
	}
	
	public long getMetricBucketId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET));
	}
	
	public void setMetricBucketId(long metricBucketId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET), metricBucketId);
	}

	public long getBucketDuration() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(BUCKET_DURATION_OFFSET));
	}
	
	public void setBucketDuration(long durationInMs) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(BUCKET_DURATION_OFFSET), durationInMs);
	}
	
	public int getMetricValueLength() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(METRIC_VALUE_LENGTH_OFFSET));
	}
	
	public void setMetricValueLength(int length) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(METRIC_VALUE_LENGTH_OFFSET), length);
		setEventSize(METRIC_VALUE_OFFSET + length);
	}
	
	public int getMetricValueOffset() {
		return getOffset(METRIC_VALUE_OFFSET);
	}
	
	public int getAvailableMetricBufferLength() {
		return getBackingArray().length - getOffset(METRIC_VALUE_OFFSET);
	}
	
}
