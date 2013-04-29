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

public class StateMetricEvent extends MetricEvent {

	private final static int INPUT_ACTIONS_OFFSET = 0;
	private final static int EVENT_ERROR_COUNT_OFFSET = 8;
	private final static int PENDING_EVENT_COUNT_OFFSET = 12;
	private static final int EVENT_SIZE = PENDING_EVENT_COUNT_OFFSET + 8;

	public StateMetricEvent() {
		super(EventType.STATE_METRIC.getId(), EVENT_SIZE);
	}
	
	public final long getInputActionsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(INPUT_ACTIONS_OFFSET));
	}

	public final void setInputActionsProcessed(long inputActionsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(INPUT_ACTIONS_OFFSET), inputActionsProcessed);
	}

	public final int getEventErrorCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getContentOffset(EVENT_ERROR_COUNT_OFFSET));
	}

	public final void setEventErrorCount(int eventErrorCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getContentOffset(EVENT_ERROR_COUNT_OFFSET), eventErrorCount);
	}
	
	public long getPendingEventCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(PENDING_EVENT_COUNT_OFFSET));
	}

	public void setPendingEventCount(long pendingEventCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(PENDING_EVENT_COUNT_OFFSET), pendingEventCount);
	}
	
}
