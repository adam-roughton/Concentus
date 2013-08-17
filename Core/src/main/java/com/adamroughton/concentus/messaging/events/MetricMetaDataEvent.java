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

import com.adamroughton.concentus.messaging.ResizingBuffer;

import static com.adamroughton.concentus.messaging.ResizingBuffer.*;

public final class MetricMetaDataEvent extends BufferBackedObject {
	
	private final Field sourceIdField = super.getBaseField().then(UUID_SIZE);
	private final Field metricIdField = sourceIdField.then(INT_SIZE);
	private final Field metricTypeField = metricIdField.then(INT_SIZE);
	private final Field isCumulativeField = metricTypeField.then(BOOL_SIZE);
	private final Field metricNameOffsetField = isCumulativeField.then(INT_SIZE);
	private final Field nameContentField = metricNameOffsetField.thenVariableLength()
			.resolveOffsets();
	
	public MetricMetaDataEvent() {
		super(EventType.METRIC_META_DATA.getId());
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
	
	public boolean getIsCumulative() {
		return getBuffer().readBoolean(isCumulativeField.offset);
	}
	
	public void setIsCumulative(boolean isCumulative) {
		getBuffer().writeBoolean(isCumulativeField.offset, isCumulative);
	}
	
	public void setNames(String referenceName, String metricName) {
		ResizingBuffer buffer = getBuffer();
		
		// clear any existing strings
		buffer.clear(nameContentField.offset);
		
		int refNameByteLength = buffer.write8BitCharString(nameContentField.offset, referenceName);
		int metricNameOffset = nameContentField.offset + refNameByteLength;
		buffer.writeInt(metricNameOffsetField.offset, metricNameOffset);
		buffer.write8BitCharString(metricNameOffset, metricName);
	}
	
	public String getReferenceName() {
		return getBuffer().read8BitCharString(nameContentField.offset);
	}
	
	public String getMetricName() {
		return getBuffer().read8BitCharString(metricNameOffsetField.offset);
	}
	
}
