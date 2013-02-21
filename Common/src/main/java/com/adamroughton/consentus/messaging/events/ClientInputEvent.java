package com.adamroughton.consentus.messaging.events;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class ClientInputEvent extends ByteArrayBackedEvent {
	
	private static final int CLIENT_ID_OFFSET = 0;
	private static final int INPUT_BUFFER_OFFSET = 16;

	public ClientInputEvent() {
		super(EventType.CLIENT_INPUT.getId());
	}
	
	public UUID getClientId() {
		long msb = MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
		long lsb = MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET + 8));
		return new UUID(msb, lsb);
	}

	public void setClientId(UUID clientId) {
		long msb = clientId.getMostSignificantBits();
		long lsb = clientId.getLeastSignificantBits();
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET), msb);
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET + 8), lsb);
	}
	
	public void copyClientId(byte[] targetBuffer, int offset) {
		System.arraycopy(getBackingArray(), getOffset(CLIENT_ID_OFFSET), targetBuffer, offset, 16);
	}
	
	public ByteBuffer getInputBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(INPUT_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
}
