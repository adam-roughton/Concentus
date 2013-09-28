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
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

public final class ClientHandlerInputEvent extends BufferBackedObject {
	
	private final Field clientHandlerIdField = super.getBaseField().then(INT_SIZE);
	private final Field reliableSeqAckField = clientHandlerIdField.then(LONG_SIZE);
	private final Field contentField = reliableSeqAckField.thenVariableLength()
			.resolveOffsets();

	public ClientHandlerInputEvent() {
		super(DataType.CLIENT_HANDLER_INPUT_EVENT);
	}
	
	@Override
	protected Field getBaseField() {
		return contentField;
	}
	
	public int getClientHandlerId() {
		return getBuffer().readInt(clientHandlerIdField.offset);
	}

	public void setClientHandlerId(int clientHandlerId) {
		getBuffer().writeInt(clientHandlerIdField.offset, clientHandlerId);
	}
	
	public long getReliableSeqAck() {
		return getBuffer().readLong(reliableSeqAckField.offset);
	}
	
	public void setReliableSeqAck(long reliableSeqAck) {
		getBuffer().writeLong(reliableSeqAckField.offset, reliableSeqAck);
	}
	
	public ResizingBuffer getContentSlice() {
		return getBuffer().slice(contentField.offset);
	}
	
	@Override
	public String toString() {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append("ClientHandlerInputEvent: ");
		if (getBuffer() == null) {
			strBuilder.append("[Not attached to a buffer]");
		} else {
			strBuilder.append("[");
			strBuilder.append("clientHandlerId=" + getClientHandlerId() + ", ");
			strBuilder.append("reliableSeqAck=" + getReliableSeqAck() + ", ");
			strBuilder.append("data=" + getContentSlice().toString());
			strBuilder.append("]");
		}
		return strBuilder.toString();
	}
	
}
