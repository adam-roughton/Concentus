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

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public class StateMetricEvent extends ByteArrayBackedEvent {

	private final static int UPDATE_ID_OFFSET = 0;
	private final static int METRIC_BUCKET_ID_OFFSET = 8;
	private final static int ACTUAL_BUCKET_DURATION_OFFSET = 16;
	private final static int INPUT_ACTIONS_OFFSET = 24;
	private final static int EVENT_ERROR_COUNT_OFFSET = 32;
	private static final int EVENT_SIZE = EVENT_ERROR_COUNT_OFFSET + 4;

	public StateMetricEvent() {
		super(EventType.STATE_METRIC.getId(), EVENT_SIZE);
	}
	
	public final long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET));
	}
	
	public final void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET), updateId);
	}
	
	public final long getMetricBucketId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET));
	}
	
	public final void setMetricBucketId(long metricBucketId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET), metricBucketId);
	}

	public final long getActualBucketDurationInMs() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(ACTUAL_BUCKET_DURATION_OFFSET));
	}
	
	public final void setActualBucketDurationInMs(long durationInMs) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(ACTUAL_BUCKET_DURATION_OFFSET), durationInMs);
	}
	
	public final long getInputActionsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_ACTIONS_OFFSET));
	}

	public final void setInputActionsProcessed(long inputActionsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_ACTIONS_OFFSET), inputActionsProcessed);
	}

	public final int getEventErrorCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET));
	}

	public final void setEventErrorCount(int eventErrorCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET), eventErrorCount);
	}
	
}
