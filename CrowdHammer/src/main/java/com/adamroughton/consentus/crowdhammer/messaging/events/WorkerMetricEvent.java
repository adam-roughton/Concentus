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
	private final static int METRIC_BUCKET_ID_OFFSET = 8;
	private final static int ACTUAL_BUCKET_DURATION_OFFSET = 16;
	private final static int INPUT_ACTIONS_OFFSET = 24;
	private final static int EVENT_ERROR_COUNT_OFFSET = 32;
	private final static int CONNECTED_CLIENT_COUNT_OFFSET = 36;

	public WorkerMetricEvent() {
		super(TestEventType.WORKER_METRIC.getId());
	}
	
	// metrics:
	/*
	 * 1. execution time avg, var, min, max, 99%, 99.99% (histogram instead?)
	 * 2. receive time for update events (histogram?) update send time - collect directly?
	 */
	
	
	public long getWorkerId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(WORKER_ID_OFFSET));
	}
	
	public void setWorkerId(long workerId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(WORKER_ID_OFFSET), workerId);
	}
	
	public long getMetricBucketId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET));
	}
	
	public void setMetricBucketId(long metricBucketId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(METRIC_BUCKET_ID_OFFSET), metricBucketId);
	}
	
	public long getSentInputActionsCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_ACTIONS_OFFSET));
	}

	public void setSentInputActionsCount(long sentInputActionsCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_ACTIONS_OFFSET), sentInputActionsCount);
	}

	public long getActualBucketDurationInMs() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(ACTUAL_BUCKET_DURATION_OFFSET));
	}

	public void setActualBucketDurationInMs(long durationInMs) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(ACTUAL_BUCKET_DURATION_OFFSET), durationInMs);
	}
	
	public int getEventErrorCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET));
	}

	public void setEventErrorCount(int eventErrorCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET), eventErrorCount);
	}
	
	public int getConnectedClientCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(CONNECTED_CLIENT_COUNT_OFFSET));
	}

	public void setConnectedClientCount(int connectedClientCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(CONNECTED_CLIENT_COUNT_OFFSET), connectedClientCount);
	}
	
}
