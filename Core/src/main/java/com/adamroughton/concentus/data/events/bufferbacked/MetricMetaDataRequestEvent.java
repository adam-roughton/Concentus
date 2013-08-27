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

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public final class MetricMetaDataRequestEvent extends BufferBackedObject {

	private final Field sourceIdField = super.getBaseField().then(UUID_SIZE);
	private final Field metricIdField = sourceIdField.then(INT_SIZE)
			.resolveOffsets();
	
	public MetricMetaDataRequestEvent() {
		super(DataType.METRIC_META_DATA_REQ_EVENT);
	}
	
	public final UUID getSourceId() {
		return getBuffer().readUUID(sourceIdField.offset);
	}
	
	public final void setSourceId(UUID sourceId) {
		getBuffer().writeUUID(sourceIdField.offset, sourceId);
	}
	
	public final int getMetricId() {
		return getBuffer().readInt(metricIdField.offset);
	}
	
	public final void setMetricId(int metricId) {
		getBuffer().writeInt(metricIdField.offset, metricId);
	}
	
}
