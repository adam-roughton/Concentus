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
