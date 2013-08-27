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
package com.adamroughton.concentus.data.events.bufferbacked;

import java.util.UUID;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public final class MetricEvent extends BufferBackedObject {

	private final Field sourceIdField = super.getBaseField().then(UUID_SIZE);
	private final Field metricIdField = sourceIdField.then(INT_SIZE);
	private final Field metricTypeField = metricIdField.then(INT_SIZE);
	private final Field metricBucketIdField = metricTypeField.then(LONG_SIZE);
	private final Field bucketDurationField = metricBucketIdField.then(LONG_SIZE);
	private final Field metricValueField = bucketDurationField.thenVariableLength()
			.resolveOffsets();
	
	public MetricEvent() {
		super(DataType.METRIC_EVENT);
	}
	
	public UUID getSourceId() {
		return getBuffer().readUUID(sourceIdField.offset);
	}
	
	public void setSourceId(UUID sourceId) {
		getBuffer().writeUUID(sourceIdField.offset, sourceId);
	}
	
	public int getMetricId() {
		return getBuffer().readInt(metricIdField.offset);
	}
	
	public void setMetricId(int metricId) {
		getBuffer().writeInt(metricIdField.offset, metricId);
	}
	
	public int getMetricType() {
		return getBuffer().readInt(metricTypeField.offset);
	}
	
	public void setMetricType(int metricType) {
		getBuffer().writeInt(metricTypeField.offset, metricType);
	}
	
	public long getMetricBucketId() {
		return getBuffer().readLong(metricBucketIdField.offset);
	}
	
	public void setMetricBucketId(long metricBucketId) {
		getBuffer().writeLong(metricBucketIdField.offset, metricBucketId);
	}

	public long getBucketDuration() {
		return getBuffer().readLong(bucketDurationField.offset);
	}
	
	public void setBucketDuration(long durationInMs) {
		getBuffer().writeLong(bucketDurationField.offset, durationInMs);
	}
	
	public ResizingBuffer getMetricValueSlice() {
		return getBuffer().slice(metricValueField.offset);
	}
	
}
