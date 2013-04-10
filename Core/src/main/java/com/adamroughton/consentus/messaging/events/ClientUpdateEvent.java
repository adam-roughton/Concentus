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
package com.adamroughton.consentus.messaging.events;

import java.nio.ByteBuffer;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class ClientUpdateEvent extends ByteArrayBackedEvent {

	private static final int CLIENT_ID_OFFSET = 0;
	private static final int UPDATE_ID_OFFSET = 8;
	private static final int SIM_TIME_OFFSET = 16;
	private static final int INPUT_UPDATE_LINK_OFFSET = 24;
	private static final int UPDATE_BUFFER_OFFSET = 40;
	
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
	
	public final InputToUpdateLink getLatestUpdateToInputMapping() {
		long updateId = MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_UPDATE_LINK_OFFSET));
		long inputActionId = MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_UPDATE_LINK_OFFSET + 8));
		return new InputToUpdateLink(updateId, inputActionId);
	}
	
	public final void setLatestUpdateToInputMapping(InputToUpdateLink entry) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_UPDATE_LINK_OFFSET), entry.getUpdateId());
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_UPDATE_LINK_OFFSET + 8), entry.getInputActionId());
	}
	
	public final ByteBuffer getUpdateBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(UPDATE_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
	public final int getUpdateOffset() {
		return getOffset(UPDATE_BUFFER_OFFSET);
	}
	
	public final int getUpdateLength() {
		byte[] backingArray = getBackingArray();
		return backingArray.length - getOffset(UPDATE_BUFFER_OFFSET);
	}
	
	public final void copyFromUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getUpdateLength();
		System.arraycopy(getBackingArray(), getOffset(UPDATE_BUFFER_OFFSET), exBuffer, 0, 
				length < updateLength? length : updateLength);
	}
	
	public final void copyToUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getUpdateLength();
		System.arraycopy(exBuffer, offset, getBackingArray(), getOffset(UPDATE_BUFFER_OFFSET), 
				length < updateLength? length : updateLength);
	}
	
	public final void addUsedLength(final ByteBuffer updateBuffer) {
		setEventSize(BASE_SIZE + getUsedLength(getOffset(UPDATE_BUFFER_OFFSET), updateBuffer));
	}
	
}
