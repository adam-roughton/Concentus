package com.adamroughton.consentus.messaging.events;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class StateMetricEvent extends ByteArrayBackedEvent {

	private final static int UPDATE_ID_OFFSET = 0;
	private final static int INPUT_ACTIONS_OFFSET = 8;
	private final static int TIME_DURATION_OFFSET = 16;
	private final static int EVENT_ERROR_COUNT_OFFSET = 24;

	public StateMetricEvent() {
		super(EventType.STATE_METRIC.getId());
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

	public long getTimeDuration() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(TIME_DURATION_OFFSET));
	}

	public void setTimeDuration(long timeDuration) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(TIME_DURATION_OFFSET), timeDuration);
	}
	
	public int getEventErrorCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET));
	}

	public void setEventErrorCount(int eventErrorCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(EVENT_ERROR_COUNT_OFFSET), eventErrorCount);
	}
	
}
