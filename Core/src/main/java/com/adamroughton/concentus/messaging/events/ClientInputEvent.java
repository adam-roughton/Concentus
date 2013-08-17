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

import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.ResizingBufferSlice;
import com.adamroughton.concentus.model.ClientId;
import static com.adamroughton.concentus.messaging.ResizingBuffer.*;

public final class ClientInputEvent extends BufferBackedObject {
	
	private final Field clientIdField = super.getBaseField().then(LONG_SIZE);
	private final Field actionIdField = clientIdField.then(LONG_SIZE);
	private final Field actionField = actionIdField.thenVariableLength()
			.resolveOffsets();

	public ClientInputEvent() {
		super(EventType.CLIENT_INPUT.getId());
	}
	
	@Override
	protected Field getBaseField() {
		return actionField;
	}
	
	public final ClientId getClientId() {
		return getBuffer().readClientId(clientIdField.offset);
	}

	public final void setClientId(final ClientId clientId) {
		getBuffer().writeClientId(clientIdField.offset, clientId);
	}
	
	public final void setClientId(final long clientIdBits) {
		getBuffer().writeLong(clientIdField.offset, clientIdBits);
	}
	
	public final long getClientIdBits() {
		return getBuffer().readLong(clientIdField.offset);
	}
	
	public final long getClientActionId() {
		return getBuffer().readLong(actionIdField.offset);
	}
	
	public final void setClientActionId(final long clientActionId) {
		getBuffer().writeLong(actionIdField.offset, clientActionId);
	}
	
	public ResizingBuffer getInputSlice() {
		return new ResizingBufferSlice(getBuffer(), actionField.offset);
	}
	
}
