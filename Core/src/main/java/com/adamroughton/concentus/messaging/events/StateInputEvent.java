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

public class StateInputEvent extends ByteArrayBackedEvent {
	
	private static final int CLIENT_HANDLER_ID_OFFSET = 0;
	private static final int INPUT_ID_OFFSET = 4;
	private static final int INPUT_BUFFER_LENGTH_OFFSET = 12;
	private static final int INPUT_BUFFER_OFFSET = 16;
	private static final int BASE_SIZE = INPUT_BUFFER_OFFSET;

	public StateInputEvent() {
		super(EventType.STATE_INPUT.getId());
	}
	
	public final int getClientHandlerId() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(CLIENT_HANDLER_ID_OFFSET));
	}

	public final void setClientHandlerId(int clientHandlerId) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(CLIENT_HANDLER_ID_OFFSET), clientHandlerId);
	}

	public final long getInputId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(INPUT_ID_OFFSET));
	}

	public final void setInputId(long inputId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(INPUT_ID_OFFSET), inputId);
	}
	
	public final int getInputBufferLength() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(INPUT_BUFFER_LENGTH_OFFSET));
	}
	
	public final int getInputOffset() {
		return getOffset(INPUT_BUFFER_OFFSET);
	}
	
	public final int getAvailableInputBufferLength() {
		return getBackingArray().length - getOffset(INPUT_BUFFER_OFFSET);
	}
	
	public final ByteBuffer getInputBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(INPUT_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
	public final int copyFromInputBytes(final byte[] exBuffer, final int offset, final int length) {
		int updateLength = getInputBufferLength();
		int copyLength = length < updateLength? length : updateLength;
		System.arraycopy(getBackingArray(), getOffset(INPUT_BUFFER_OFFSET), exBuffer, offset, 
				copyLength);
		return copyLength;
	}
	
	public final int copyToInputBytes(final byte[] exBuffer, final int offset, final int length) {
		byte[] backingArray = getBackingArray();
		int maxLength = backingArray.length - getOffset(INPUT_BUFFER_OFFSET);
		int copyLength = length < maxLength? length : maxLength;
		System.arraycopy(exBuffer, offset, getBackingArray(), getOffset(INPUT_BUFFER_OFFSET), 
				copyLength);
		return copyLength;
	}
	
	public final void setUsedLength(final ByteBuffer inputBuffer) {
		int usedLength = getUsedLength(getOffset(INPUT_BUFFER_OFFSET), inputBuffer);
		setUsedLength(usedLength);
	}
	
	public final void setUsedLength(int length) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(INPUT_BUFFER_LENGTH_OFFSET), length);
		setEventSize(BASE_SIZE + length);
		
	}
	
}
