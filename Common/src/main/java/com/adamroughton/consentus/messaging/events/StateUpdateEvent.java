package com.adamroughton.consentus.messaging.events;

import java.nio.ByteBuffer;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class StateUpdateEvent extends ByteArrayBackedEvent {

	private static final int UPDATE_ID_OFFSET = 0;
	private static final int SIM_TIME_OFFSET = 8;
	private static final int UPDATE_BUFFER_OFFSET = 16;
	
	public StateUpdateEvent() {
		super(EventType.STATE_UPDATE.getId());
	}
	
	public long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET));
	}

	public void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET), updateId);
	}

	public long getSimTime() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(SIM_TIME_OFFSET));
	}

	public void setSimTime(long simTime) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(SIM_TIME_OFFSET), simTime);
	}
	
	public ByteBuffer getUpdateBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(UPDATE_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
}
