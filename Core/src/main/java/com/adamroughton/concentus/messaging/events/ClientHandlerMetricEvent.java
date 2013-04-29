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

public class ClientHandlerMetricEvent extends MetricEvent {

	private final static int INPUT_ACTIONS_PROCESSED_OFFSET = 0;
	private final static int ACTIVE_CLIENT_COUNT = 8;
	private final static int EVENT_ERROR_COUNT_OFFSET = 16;
	private final static int PENDING_EVENT_COUNT_OFFSET = 20;
	private final static int TOTAL_EVENTS_PROCESSED_OFFSET = 28;
	private final static int CONNECT_REQUESTS_PROCESSED_OFFSET = 36;
	private final static int UPDATE_EVENT_PROCESSED_COUNT_OFFSET = 44;
	private final static int UPDATE_INFO_PROCESSED_COUNT_OFFSET = 52;
	private final static int SENT_UPDATE_COUNT_OFFSET = 60;
	private static final int EVENT_SIZE = SENT_UPDATE_COUNT_OFFSET + 8;

	public ClientHandlerMetricEvent() {
		super(EventType.CLIENT_HANDLER_METRIC.getId(), EVENT_SIZE);
	}
	
	public final long getInputActionsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(INPUT_ACTIONS_PROCESSED_OFFSET));
	}

	public final void setInputActionsProcessed(long inputActionsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(INPUT_ACTIONS_PROCESSED_OFFSET), inputActionsProcessed);
	}
	
	public final long getActiveClientCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(ACTIVE_CLIENT_COUNT));
	}

	public final void setActiveClientCount(long activeClientCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(ACTIVE_CLIENT_COUNT), activeClientCount);
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
	
	public final long getTotalEventsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(TOTAL_EVENTS_PROCESSED_OFFSET));
	}

	public final void setTotalEventsProcessed(long totalEventsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(TOTAL_EVENTS_PROCESSED_OFFSET), totalEventsProcessed);
	}
	
	public final long getConnectRequestsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(CONNECT_REQUESTS_PROCESSED_OFFSET));
	}

	public final void setConnectionRequestsProcessed(long connectionRequestsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(CONNECT_REQUESTS_PROCESSED_OFFSET), connectionRequestsProcessed);
	}
	
	public final long getUpdateEventsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(UPDATE_EVENT_PROCESSED_COUNT_OFFSET));
	}

	public final void setUpdateEventsProcessed(long updateEventsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(UPDATE_EVENT_PROCESSED_COUNT_OFFSET), updateEventsProcessed);
	}
	
	public final long getUpdateInfoEventsProcessed() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(UPDATE_INFO_PROCESSED_COUNT_OFFSET));
	}

	public final void setUpdateInfoEventsProcessed(long updateInfoEventsProcessed) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(UPDATE_INFO_PROCESSED_COUNT_OFFSET), updateInfoEventsProcessed);
	}
	
	public final long getSentUpdateCount() {
		return MessageBytesUtil.readLong(getBackingArray(), getContentOffset(SENT_UPDATE_COUNT_OFFSET));
	}

	public final void setSentUpdateCount(long sentUpdateCount) {
		MessageBytesUtil.writeLong(getBackingArray(), getContentOffset(SENT_UPDATE_COUNT_OFFSET), sentUpdateCount);
	}
	
}
