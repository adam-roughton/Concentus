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

public final class ClientUpdateEvent extends BufferBackedObject {

	private final Field clientIdField = super.getBaseField().then(LONG_SIZE);
	private final Field updateIdField = clientIdField.then(LONG_SIZE);
	private final Field simTimeField = updateIdField.then(LONG_SIZE);
	private final Field highestInputActionIdField = simTimeField.then(LONG_SIZE);
	private final Field updateField = highestInputActionIdField.thenVariableLength()
			.resolveOffsets();
	
	// 2 bytes for last client action Id, 2 bytes for preceding 16 updates (flag based)
	
	public ClientUpdateEvent() {
		super(EventType.CLIENT_UPDATE.getId());
	}
	
	public long getClientId() {
		return getBuffer().readLong(clientIdField.offset);
	}

	public final void setClientId(long clientId) {
		getBuffer().writeLong(clientIdField.offset, clientId);
	}
	
	public final long getUpdateId() {
		return getBuffer().readLong(updateIdField.offset);
	}

	public final void setUpdateId(long updateId) {
		getBuffer().writeLong(updateIdField.offset, updateId);
	}

	public final long getSimTime() {
		return getBuffer().readLong(simTimeField.offset);
	}

	public final void setSimTime(long simTime) {
		getBuffer().writeLong(simTimeField.offset, simTime);
	}
	
	public final long getHighestInputActionId() {
		return getBuffer().readLong(highestInputActionIdField.offset);
	}
	
	public final void setHighestInputActionId(long inputActionId) {
		getBuffer().writeLong(highestInputActionIdField.offset, inputActionId);
	}
	
	public ResizingBuffer getUpdateSlice() {
		return getBuffer().slice(updateField.offset);
	}
	
}
