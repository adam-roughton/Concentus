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
import com.adamroughton.consentus.model.ClientId;

public class ClientInputEvent extends ByteArrayBackedEvent {
	
	private static final int CLIENT_ID_OFFSET = 0;
	private static final int CLIENT_ACTION_ID_OFFSET = 8;
	private static final int INPUT_BUFFER_OFFSET = 16;
	
	private static final int BASE_SIZE = 16;

	public ClientInputEvent() {
		super(EventType.CLIENT_INPUT.getId());
	}
	
	public final ClientId getClientId() {
		return MessageBytesUtil.readClientId(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}

	public final void setClientId(final ClientId clientId) {
		MessageBytesUtil.writeClientId(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientId);
	}
	
	public final void setClientId(final long clientIdBits) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientIdBits);
	}
	
	public final long getClientIdBits() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}
	
	public final long getClientActionId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ACTION_ID_OFFSET));
	}
	
	public final void setClientActionId(final long clientActionId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ACTION_ID_OFFSET), clientActionId);
	}
	
	public final ByteBuffer getInputBuffer() {
		byte[] backingArray = getBackingArray();
		int offset = getOffset(INPUT_BUFFER_OFFSET);
		return ByteBuffer.wrap(backingArray, offset, backingArray.length - offset);
	}
	
	public final void addUsedLength(final ByteBuffer inputBuffer) {
		setEventSize(BASE_SIZE + getUsedLength(getOffset(INPUT_BUFFER_OFFSET), inputBuffer));
	}
	
}
