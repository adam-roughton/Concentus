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

public final class ClientHandlerUpdateEvent extends BufferBackedObject {

	private final Field clientHandlerIdField = super.getBaseField().then(INT_SIZE);
	private final Field actionCollectorIdField = clientHandlerIdField.then(INT_SIZE);
	private final Field reliableSeqField = actionCollectorIdField.then(LONG_SIZE);
	private final Field contentField = reliableSeqField.thenVariableLength()
			.resolveOffsets();
	
	public ClientHandlerUpdateEvent() {
		super(DataType.CLIENT_HANDLER_UPDATE_EVENT);
	}
	
	public int getClientHandlerId() {
		return getBuffer().readInt(clientHandlerIdField.offset);
	}

	public void setClientHandlerId(int clientHandlerId) {
		getBuffer().writeInt(clientHandlerIdField.offset, clientHandlerId);
	}
	
	/**
	 * The action collector that processed the action.
	 * @return the ID of the action collector that processed the action.
	 */
	public int getActionCollectorId() {
		return getBuffer().readInt(actionCollectorIdField.offset);
	}
	
	public void setActionCollectorId(int actionCollectorId) {
		getBuffer().writeInt(actionCollectorIdField.offset, actionCollectorId);
	}
	
	/**
	 * The sequence number of this update for the given client handler - action
	 * collector pairing. 
	 * @return
	 */
	public long getReliableSeq() {
		return getBuffer().readLong(reliableSeqField.offset);
	}
	
	public void setReliableSeq(long reliableSeq) {
		getBuffer().writeLong(reliableSeqField.offset, reliableSeq);
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
