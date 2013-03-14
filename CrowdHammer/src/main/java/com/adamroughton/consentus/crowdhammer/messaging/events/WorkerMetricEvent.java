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
package com.adamroughton.consentus.crowdhammer.messaging.events;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public class WorkerMetricEvent extends ByteArrayBackedEvent {

	private final static int WORKER_ID_OFFSET = 0;
	private final static int INPUT_ACTIONS_OFFSET = 8;
	private final static int DURATION_OFFSET = 16;
	private final static int EVENT_ERROR_COUNT_OFFSET = 24;

	public WorkerMetricEvent() {
		super(TestEventType.STATE_METRIC.getId());
	}
	
	// metrics:
	/*
	 * 1. execution time avg, var, min, max, 99%, 99.99% (histogram instead?)
	 * 2. receive time for update events (histogram?) update send time - collect directly?
	 */
	
	
	public long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(WORKER_ID_OFFSET));
	}
	
	public void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(WORKER_ID_OFFSET), updateId);
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
