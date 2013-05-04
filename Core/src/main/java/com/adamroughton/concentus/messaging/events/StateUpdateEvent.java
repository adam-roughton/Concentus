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

public class StateUpdateEvent extends ByteArrayBackedEvent {

	private static final int UPDATE_ID_OFFSET = 0;
	private static final int SIM_TIME_OFFSET = 8;
	private static final int UPDATE_BUFFER_LENGTH_OFFSET = 16;
	private static final int UPDATE_BUFFER_OFFSET = 20;
	private static final int BASE_SIZE = UPDATE_BUFFER_OFFSET;
	
	public StateUpdateEvent() {
		super(EventType.STATE_UPDATE.getId());
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
	
	public final int getByteCountInBuffer() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(UPDATE_BUFFER_LENGTH_OFFSET));
	}
	
	public final int getMaxUpdateBufferLength() {
		return getBackingArray().length - getOffset(UPDATE_BUFFER_OFFSET);
	}
	
	public final ByteBuffer getUpdateBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(UPDATE_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
	public final int copyUpdateBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getByteCountInBuffer();
		int copyLength = length < updateLength? length : updateLength;
		System.arraycopy(getBackingArray(), getOffset(UPDATE_BUFFER_OFFSET), exBuffer, offset, 
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
