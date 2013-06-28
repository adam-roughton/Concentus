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

public final class MetricMetaDataRequestEvent extends ByteArrayBackedEvent {

	private static final int SOURCE_ID_OFFSET = 0;
	private static final int METRIC_ID_OFFSET = 16;
	private static final int LENGTH = METRIC_ID_OFFSET + 4;
	
	public MetricMetaDataRequestEvent() {
		super(EventType.METRIC_META_DATA_REQ.getId(), LENGTH);
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
	
}
