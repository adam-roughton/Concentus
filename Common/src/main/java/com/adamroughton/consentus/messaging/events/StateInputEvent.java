package com.adamroughton.consentus.messaging.events;

import java.nio.ByteBuffer;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class StateInputEvent extends ByteArrayBackedEvent {
	
	private static final int CLIENT_HANDLER_ID_OFFSET = 0;
	private static final int INPUT_ID_OFFSET = 4;
	private static final int INPUT_BUFFER_OFFSET = 12;

	public StateInputEvent() {
		super(EventType.STATE_INPUT.getId());
	}
	
	public int getClientHandlerId() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(CLIENT_HANDLER_ID_OFFSET));
	}

	public void setClientHandlerId(int clientHandlerId) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(CLIENT_HANDLER_ID_OFFSET), clientHandlerId);
	}

	public long getInputId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_ID_OFFSET));
	}

	public void setInputId(long inputId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_ID_OFFSET), inputId);
	}
	
	public ByteBuffer getInputBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(INPUT_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
}
