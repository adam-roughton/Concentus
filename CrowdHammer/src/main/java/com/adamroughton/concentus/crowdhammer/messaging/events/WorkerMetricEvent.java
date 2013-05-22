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
package com.adamroughton.concentus.crowdhammer.messaging.events;

import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.events.MetricEvent;
import com.adamroughton.concentus.util.RunningStats;

public class WorkerMetricEvent extends MetricEvent {

	private final static int INPUT_ACTIONS_OFFSET = 0;
	private final static int EVENT_ERROR_COUNT_OFFSET = 8;
	private final static int CONNECTED_CLIENT_COUNT_OFFSET = 12;
	private final static int PENDING_EVENT_COUNT_OFFSET = 16;
	private final static int INPUT_TO_UPDATE_LATENCY_OFFSET = 24;
	private final static int INPUT_TO_UPDATE_LATENCY_LATE_COUNT_OFFSET = 60;
	
	private final static int EVENT_SIZE = INPUT_TO_UPDATE_LATENCY_LATE_COUNT_OFFSET + 8;

	public WorkerMetricEvent() {
		super(TestEventType.WORKER_METRIC.getId(), EVENT_SIZE);
	}
	
	// metrics:
	/*
	 * 1. execution time avg, var, min, max, 99%, 99.99% (histogram instead?)
	 * 2. receive time for update events (histogram?) update send time - collect directly?
	 */
	
	public long getSentInputActionsCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(INPUT_ACTIONS_OFFSET));
	}

	public void setSentInputActionsCount(long sentInputActionsCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(INPUT_ACTIONS_OFFSET), sentInputActionsCount);
	}
	
	public int getEventErrorCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getContentOffset(EVENT_ERROR_COUNT_OFFSET));
	}

	public void setEventErrorCount(int eventErrorCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getContentOffset(EVENT_ERROR_COUNT_OFFSET), eventErrorCount);
	}
	
	public int getConnectedClientCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getContentOffset(CONNECTED_CLIENT_COUNT_OFFSET));
	}

	public void setConnectedClientCount(int connectedClientCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getContentOffset(CONNECTED_CLIENT_COUNT_OFFSET), connectedClientCount);
	}
	
	public long getPendingEventCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(PENDING_EVENT_COUNT_OFFSET));
	}

	public void setPendingEventCount(long pendingEventCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(PENDING_EVENT_COUNT_OFFSET), pendingEventCount);
	}
	
	public RunningStats getInputToUpdateLatency() {
		int offset = getContentOffset(INPUT_TO_UPDATE_LATENCY_OFFSET);
		int count = MessageBytesUtil.readInt(getBackingArray(), offset);
		offset += 4;
		double mean = MessageBytesUtil.readDouble(getBackingArray(), offset);
		offset += 8;
		double sumSqrs = MessageBytesUtil.readDouble(getBackingArray(), offset);
		offset += 8;
		double min = MessageBytesUtil.readDouble(getBackingArray(), offset);
		offset += 8;
		double max = MessageBytesUtil.readDouble(getBackingArray(), offset);
		return new RunningStats(count, mean, sumSqrs, min, max);
	}
	
	public void setInputToUpdateLatency(RunningStats inputToUpdateLatency) {
		int offset = getContentOffset(INPUT_TO_UPDATE_LATENCY_OFFSET);
		MessageBytesUtil.writeInt(getBackingArray(), offset, inputToUpdateLatency.getCount());
		offset += 4;
		MessageBytesUtil.writeDouble(getBackingArray(), offset, inputToUpdateLatency.getMean());
		offset += 8;
		MessageBytesUtil.writeDouble(getBackingArray(), offset, inputToUpdateLatency.getSumOfSquares());
		offset += 8;
		MessageBytesUtil.writeDouble(getBackingArray(), offset, inputToUpdateLatency.getMin());
		offset += 8;
		MessageBytesUtil.writeDouble(getBackingArray(), offset, inputToUpdateLatency.getMax());
	}
	
	public long getInputToUpdateLatencyLateCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_LATE_COUNT_OFFSET));
	}

	public void setInputToUpdateLatencyLateCount(long inputToUpdateLatencyLateCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_LATE_COUNT_OFFSET), inputToUpdateLatencyLateCount);
	}
	
}
