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

public class WorkerMetricEvent extends MetricEvent {

	private final static int INPUT_ACTIONS_OFFSET = 0;
	private final static int EVENT_ERROR_COUNT_OFFSET = 8;
	private final static int CONNECTED_CLIENT_COUNT_OFFSET = 12;
	private final static int PENDING_EVENT_COUNT_OFFSET = 16;
	private final static int INPUT_TO_UPDATE_LATENCY_COUNT_OFFSET = 24;
	private final static int INPUT_TO_UPDATE_LATENCY_MEAN_OFFSET = 28;
	private final static int INPUT_TO_UPDATE_LATENCY_SUM_SQRS_OFFSET = 36;
	private final static int INPUT_TO_UPDATE_LATENCY_MIN_OFFSET = 44;
	private final static int INPUT_TO_UPDATE_LATENCY_MAX_OFFSET = 52;
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
	
	public int getInputToUpdateLatencyCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_COUNT_OFFSET));
	}
	
	public void setInputToUpdateLatencyCount(int inputToUpdateLatencyCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_COUNT_OFFSET), inputToUpdateLatencyCount);
	}
	
	public double getInputToUpdateLatencyMean() {
		return MessageBytesUtil.readDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_MEAN_OFFSET));
	}
	
	public void setInputToUpdateLatencyMean(double inputToUpdateLatencyMean) {
		MessageBytesUtil.writeDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_MEAN_OFFSET), inputToUpdateLatencyMean);
	}
	
	public double getInputToUpdateLatencySumSqrs() {
		return MessageBytesUtil.readDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_SUM_SQRS_OFFSET));
	}
	
	public void setInputToUpdateLatencySumSqrs(double inputToUpdateLatencySumSqrs) {
		MessageBytesUtil.writeDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_SUM_SQRS_OFFSET), inputToUpdateLatencySumSqrs);
	}
	
	public double getInputToUpdateLatencyMin() {
		return MessageBytesUtil.readDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_MIN_OFFSET));
	}

	public void setInputToUpdateLatencyMin(double inputToUpdateLatencyMin) {
		MessageBytesUtil.writeDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_MIN_OFFSET), inputToUpdateLatencyMin);
	}
	
	public double getInputToUpdateLatencyMax() {
		return MessageBytesUtil.readDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_MAX_OFFSET));
	}

	public void setInputToUpdateLatencyMax(double inputToUpdateLatencyMax) {
		MessageBytesUtil.writeDouble(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_MAX_OFFSET), inputToUpdateLatencyMax);
	}
	
	public long getInputToUpdateLatencyLateCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_LATE_COUNT_OFFSET));
	}

	public void setInputToUpdateLatencyLateCount(long inputToUpdateLatencyLateCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(INPUT_TO_UPDATE_LATENCY_LATE_COUNT_OFFSET), inputToUpdateLatencyLateCount);
	}
	
}
