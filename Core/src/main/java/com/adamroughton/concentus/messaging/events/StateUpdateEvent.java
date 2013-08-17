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

public class StateUpdateEvent extends BufferBackedObject {

	private final Field updateIdField = super.getBaseField().then(LONG_SIZE);
	private final Field simTimeField = updateIdField.then(LONG_SIZE);
	private final Field updateContentField = simTimeField.thenVariableLength()
			.resolveOffsets();
	
	public StateUpdateEvent() {
		super(EventType.STATE_UPDATE.getId());
	}
	
	public long getUpdateId() {
		return getBuffer().readLong(updateIdField.offset);
	}

	public void setUpdateId(long updateId) {
		getBuffer().writeLong(updateIdField.offset, updateId);
	}

	public long getSimTime() {
		return getBuffer().readLong(simTimeField.offset);
	}

	public void setSimTime(long simTime) {
		getBuffer().writeLong(simTimeField.offset, simTime);
	}
	
	public ResizingBuffer getContentSlice() {
		return getBuffer().slice(updateContentField.offset);
	}
	
}
