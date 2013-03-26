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
package com.adamroughton.consentus.messaging.events;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class StateMetricEvent extends ByteArrayBackedEvent {

	private final static int UPDATE_ID_OFFSET = 0;
	private final static int INPUT_ACTIONS_OFFSET = 8;
	private final static int DURATION_OFFSET = 16;
	private final static int EVENT_ERROR_COUNT_OFFSET = 24;
	private static final int EVENT_SIZE = EVENT_ERROR_COUNT_OFFSET + 4;

	public StateMetricEvent() {
		super(EventType.STATE_METRIC.getId(), EVENT_SIZE);
	}
	
	public long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET));
	}
	
	public void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET), updateId);
	}
	
	public long getInputActionsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_ACTIONS_OFFSET));
	}

	public void setInputActionsProcessed(long inputActionsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_ACTIONS_OFFSET), inputActionsProcessed);
	}

	public long getDurationInMs() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(DURATION_OFFSET));
	}

	public void setDurationInMs(long durationInMs) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(DURATION_OFFSET), durationInMs);
	}
	
	public int getEventErrorCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET));
	}

	public void setEventErrorCount(int eventErrorCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET), eventErrorCount);
	}
	
}
