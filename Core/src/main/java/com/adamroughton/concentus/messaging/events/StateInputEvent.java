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

import static com.adamroughton.concentus.messaging.ResizingBuffer.*;

import com.adamroughton.concentus.messaging.ResizingBuffer;

public final class StateInputEvent extends BufferBackedObject {
	
	private final Field clientHandlerIdField = super.getBaseField().then(INT_SIZE);
	private final Field inputIdField = clientHandlerIdField.then(LONG_SIZE);
	private final Field isHeartbeatField = inputIdField.then(BOOL_SIZE);
	private final Field inputField = isHeartbeatField.thenVariableLength()
			.resolveOffsets();

	public StateInputEvent() {
		super(EventType.STATE_INPUT.getId());
	}
	
	public int getClientHandlerId() {
		return getBuffer().readInt(clientHandlerIdField.offset);
	}

	public void setClientHandlerId(int clientHandlerId) {
		getBuffer().writeInt(clientHandlerIdField.offset, clientHandlerId);
	}

	public long getInputId() {
		return getBuffer().readLong(inputIdField.offset);
	}

	public void setInputId(long inputId) {
		getBuffer().writeLong(inputIdField.offset, inputId);
	}
	
	public boolean isHeartbeat() {
		return getBuffer().readBoolean(isHeartbeatField.offset);
	}
	
	public void setIsHeartbeat(boolean isHeartbeat) {
		getBuffer().writeBoolean(isHeartbeatField.offset, isHeartbeat);
	}
	
	public ResizingBuffer getInputSlice() {
		return getBuffer().slice(inputField.offset);
	}
	
}
