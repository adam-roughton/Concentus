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

	private static final int UPDATE_ID_OFFSET = 0;
	private static final int SIM_TIME_OFFSET = 8;
	private static final int UPDATE_BUFFER_OFFSET = 16;
	
	public ClientUpdateEvent() {
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
	
	public int getUpdateOffset() {
		return getOffset(UPDATE_BUFFER_OFFSET);
	}
	
	public int getUpdateLength() {
		byte[] backingArray = getBackingArray();
		return backingArray.length - getOffset(UPDATE_BUFFER_OFFSET);
	}
	
	public void copyFromUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getUpdateLength();
		System.arraycopy(getBackingArray(), UPDATE_BUFFER_OFFSET, exBuffer, 0, 
				length < updateLength? length : updateLength);
	}
	
	public void copyToUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getUpdateLength();
		System.arraycopy(exBuffer, offset, getBackingArray(), UPDATE_BUFFER_OFFSET, 
				length < updateLength? length : updateLength);
	}
	
}
