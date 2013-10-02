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
package com.adamroughton.concentus.data.events.bufferbacked;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.ChunkReader;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.ClientId;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public final class ClientInputEvent extends BufferBackedObject {
	
	private final Field clientIdField = super.getBaseField().then(LONG_SIZE);
	private final Field reliableSeqAckField = clientIdField.then(LONG_SIZE);
	private final Field hasActionField = reliableSeqAckField.then(BOOL_SIZE);
	private final Field actionField = hasActionField.thenVariableLength()
			.resolveOffsets();

	public ClientInputEvent() {
		super(DataType.CLIENT_INPUT_EVENT);
	}
	
	@Override
	protected Field getBaseField() {
		return actionField;
	}
	
	public ClientId getClientId() {
		return getBuffer().readClientId(clientIdField.offset);
	}

	public void setClientId(ClientId clientId) {
		getBuffer().writeClientId(clientIdField.offset, clientId);
	}
	
	public void setClientId(long clientIdBits) {
		getBuffer().writeLong(clientIdField.offset, clientIdBits);
	}
	
	public long getClientIdBits() {
		return getBuffer().readLong(clientIdField.offset);
	}
	
	public long getReliableSeqAck() {
		return getBuffer().readLong(reliableSeqAckField.offset);
	}
	
	public void setReliableSeqAck(long reliableSeqAck) {
		getBuffer().writeLong(reliableSeqAckField.offset, reliableSeqAck);
	}
	
	public boolean hasActions() {
		return getBuffer().readBoolean(hasActionField.offset);
	}
	
	public void setHasActions(boolean hasActions) {
		getBuffer().writeBoolean(hasActionField.offset, hasActions);
	}
	
	public ChunkReader getActions() {
		return new ChunkReader(getBuffer(), actionField.offset);
	}
	
	public ChunkWriter getActionsWriter() {
		return new ChunkWriter(getBuffer(), actionField.offset);
	}
	
	public ResizingBuffer getActionsSlice() {
		return getBuffer().slice(actionField.offset);
	}
	
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("ClientInputEvent: ");
		if (getBuffer() == null) {
			strBuilder.append("[Not attached to a buffer]");
		} else {
			strBuilder.append("[");
			strBuilder.append("clientId=" + getClientIdBits() + ", ");
			strBuilder.append("reliableSeqAck=" + getReliableSeqAck() + ", ");
			strBuilder.append("hasAction=" + hasActions() + ", ");
			strBuilder.append("actionsData=" + getActionsSlice().toString());
			strBuilder.append("]");
		}
		return strBuilder.toString();
	}
	
}
