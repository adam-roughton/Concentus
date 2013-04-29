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

import java.nio.ByteBuffer;

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public class ClientUpdateEvent extends ByteArrayBackedEvent {

	private static final int CLIENT_ID_OFFSET = 0;
	private static final int UPDATE_ID_OFFSET = 8;
	private static final int SIM_TIME_OFFSET = 16;
	private static final int HIGHEST_INPUT_ACTION_ID_OFFSET = 24;
	private static final int UPDATE_BUFFER_LENGTH_OFFSET = 32;
	private static final int UPDATE_BUFFER_OFFSET = 36;
	
	private static final int BASE_SIZE = UPDATE_BUFFER_OFFSET;
	
	// 2 bytes for last client action Id, 2 bytes for preceding 16 updates (flag based)
	
	public ClientUpdateEvent() {
		super(EventType.CLIENT_UPDATE.getId());
	}
	
	public final long getClientId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}

	public final void setClientId(long clientId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientId);
	}
	
	public final long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET));
	}

	public final void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET), updateId);
	}

	public final long getSimTime() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(SIM_TIME_OFFSET));
	}

	public final void setSimTime(long simTime) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(SIM_TIME_OFFSET), simTime);
	}
	
	public final long getHighestInputActionId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(HIGHEST_INPUT_ACTION_ID_OFFSET));
	}
	
	public final void setHighestInputActionId(long inputActionId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(HIGHEST_INPUT_ACTION_ID_OFFSET), inputActionId);
	}
	
	public final ByteBuffer getUpdateBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(UPDATE_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
	public final int getUpdateOffset() {
		return getOffset(UPDATE_BUFFER_OFFSET);
	}
	
	public final int getUpdateBufferLength() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(UPDATE_BUFFER_LENGTH_OFFSET));
	}
	
	public final int copyFromUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getUpdateBufferLength();
		int copyLength = length < updateLength? length : updateLength;
		System.arraycopy(getBackingArray(), getOffset(UPDATE_BUFFER_OFFSET), exBuffer, 0, 
				copyLength);
		return copyLength;
	}
	
	public final int copyToUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		byte[] backingArray = getBackingArray();
		int maxLength = backingArray.length - getOffset(UPDATE_BUFFER_OFFSET);
		int copyLength = length < maxLength? length : maxLength;
		System.arraycopy(exBuffer, offset, getBackingArray(), getOffset(UPDATE_BUFFER_OFFSET), 
				copyLength);
		return copyLength;
	}
	
	public final void setUsedLength(final ByteBuffer updateBuffer) {
		int usedLength = getUsedLength(getOffset(UPDATE_BUFFER_OFFSET), updateBuffer);
		setUsedLength(usedLength);
	}
	
	public final void setUsedLength(int length) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(UPDATE_BUFFER_LENGTH_OFFSET), length);
		setEventSize(BASE_SIZE + length);
	}
	
}
