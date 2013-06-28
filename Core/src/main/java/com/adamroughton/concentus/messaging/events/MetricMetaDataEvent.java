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

public final class MetricMetaDataEvent extends ByteArrayBackedEvent {

	private static final int SOURCE_ID_OFFSET = 0;
	private static final int METRIC_ID_OFFSET = 16;
	private static final int METRIC_TYPE_OFFSET = 20;
	private static final int REFERENCE_NAME_OFFSET = 24;
	private static final int METRIC_NAME_OFFSET = 125;
	private static final int LENGTH = METRIC_NAME_OFFSET + 101;
	
	public MetricMetaDataEvent() {
		super(EventType.METRIC_META_DATA.getId(), LENGTH);
	}
	
	public final UUID getSourceId() {
		return MessageBytesUtil.readUUID(getBackingArray(), getOffset(SOURCE_ID_OFFSET));
	}
	
	public final void setSourceId(UUID sourceId) {
		MessageBytesUtil.writeUUID(getBackingArray(), getOffset(SOURCE_ID_OFFSET), sourceId);
	}
	
	public final int getMetricId() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(METRIC_ID_OFFSET));
	}
	
	public final void setMetricId(int metricId) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(METRIC_ID_OFFSET), metricId);
	}
	
	public final int getMetricType() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(METRIC_TYPE_OFFSET));
	}
	
	public final void setMetricType(int metricType) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(METRIC_TYPE_OFFSET), metricType);
	}
	
	public final String getReferenceName() {
		return MessageBytesUtil.read8BitCharString(getBackingArray(), REFERENCE_NAME_OFFSET);
	}
	
	public final void setReference(String referenceName) {
		if (referenceName.length() > 100) 
			throw new IllegalArgumentException(String.format("The referenceName can have up to a maximum " +
					"of 100 characters (was %d).", referenceName.length()));
		MessageBytesUtil.write8BitCharString(getBackingArray(), getOffset(REFERENCE_NAME_OFFSET), referenceName);
	}
	
	public final String getMetricName() {
		return MessageBytesUtil.read8BitCharString(getBackingArray(), METRIC_NAME_OFFSET);
	}
	
	public final void setMetricName(String metricName) {
		if (metricName.length() > 100) 
			throw new IllegalArgumentException(String.format("The metricName can have up to a maximum " +
					"of 100 characters (was %d).", metricName.length()));
		MessageBytesUtil.write8BitCharString(getBackingArray(), getOffset(METRIC_NAME_OFFSET), metricName);
	}
	
	
}
