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

import static com.adamroughton.concentus.data.ResizingBuffer.*;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.ChunkReader;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;

public final class ClientUpdateEvent extends BufferBackedObject {

	public static int ACK_FIELD_LENGTH = 4;
	
	private final Field clientIdField = super.getBaseField().then(LONG_SIZE);
	
	/*
	 * Use 4 bytes to ACK or NACK the last 32 received actions. The base Id field
	 * is the start action ID for the flag field, while each raised bit signals 
	 * the NACK of the (baseId + bit index) action ID.
	 */
	private final Field actionAckFlagsHeadIdField = clientIdField.then(LONG_SIZE);
	private final Field actionAckFlagsField = actionAckFlagsHeadIdField.then(ACK_FIELD_LENGTH); 
	
	private final Field contentField = actionAckFlagsField.thenVariableLength()
			.resolveOffsets();
	
	public ClientUpdateEvent() {
		super(DataType.CLIENT_UPDATE_EVENT);
	}
	
	public long getClientId() {
		return getBuffer().readLong(clientIdField.offset);
	}

	public final void setClientId(long clientId) {
		getBuffer().writeLong(clientIdField.offset, clientId);
	}
	
	/**
	 * Gets the ID of the last received action that sits at the head
	 * of the ACK flags field. This action id is <b>NOT</b> included in the 
	 * ACK flags field itself, with the final ACK action ID in the field
	 * equal to this head ID - 1; 
	 * @return
	 */
	public long getActionAckFlagsHeadId() {
		return getBuffer().readLong(actionAckFlagsHeadIdField.offset);
	}
	
	public void setActionAckFlagsHeadId(long actionAckFlagsHeadId) {
		getBuffer().writeLong(actionAckFlagsHeadIdField.offset, actionAckFlagsHeadId);
	}
	
	public boolean hasNacks() {
		return getBuffer().readInt(actionAckFlagsField.offset) != 0;
	}
	
	public int getAckFieldLength() {
		return actionAckFlagsField.size * 8;
	}
	
	public boolean getNackFlagAtIndex(int index) {
		return getBuffer().readFlag(actionAckFlagsField.offset, 4, index);
	}
	
	public void setNackFlagAtIndex(int index, boolean isNack) {
		getBuffer().writeFlag(actionAckFlagsField.offset, 4, index, isNack);
	}
	
	public ChunkReader getChunkedContent() {
		return new ChunkReader(getBuffer(), contentField.offset);
	}
	
	public ChunkWriter newChunkedContentWriter() {
		return new ChunkWriter(getBuffer(), contentField.offset);
	}
	
	public ResizingBuffer getChunkedContentSlice() {
		return getBuffer().slice(contentField.offset);
	}
	
}
