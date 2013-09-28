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

public final class ActionReceiptEvent extends BufferBackedObject {

	private final Field actionCollectorIdField = super.getBaseField().then(INT_SIZE);
	private final Field reliableSeqField = actionCollectorIdField.then(LONG_SIZE);
	private final Field clientIdField = reliableSeqField.then(LONG_SIZE);
	private final Field actionIdField = clientIdField.then(LONG_SIZE);
	private final Field startTimeField = actionIdField.then(LONG_SIZE);
	private final Field effectDataField = startTimeField.thenVariableLength()
			.resolveOffsets();
	
	public ActionReceiptEvent() {
		super(DataType.ACTION_RECEIPT_EVENT);
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
	 * The sequence number of this receipt for the given client handler - action
	 * collector pairing. 
	 * @return
	 */
	public long getReliableSeq() {
		return getBuffer().readLong(reliableSeqField.offset);
	}
	
	public void setReliableSeq(long reliableSeq) {
		getBuffer().writeLong(reliableSeqField.offset, reliableSeq);
	}
	
	public long getClientIdBits() {
		return getBuffer().readLong(clientIdField.offset);
	}
	
	public void setClientIdBits(long clientIdBits) {
		getBuffer().writeLong(clientIdField.offset, clientIdBits);
	}
	
	public ClientId getClientId() {
		return getBuffer().readClientId(clientIdField.offset);
	}
	
	public void setClientId(ClientId clientId) {
		getBuffer().writeClientId(clientIdField.offset, clientId);
	}
	
	public long getActionId() {
		return getBuffer().readLong(actionIdField.offset);
	}
	
	public void setActionId(long actionId) {
		getBuffer().writeLong(actionIdField.offset, actionId);
	}
	
	public long getStartTime() {
		return getBuffer().readLong(startTimeField.offset);
	}
	
	public void setStartTime(long startTime) {
		getBuffer().writeLong(startTimeField.offset, startTime);
	}
	
	public ChunkReader getEffects() {
		return new ChunkReader(getBuffer(), effectDataField.offset);
	}
	
	public ChunkWriter getEffectsWriter() {
		return new ChunkWriter(getBuffer(), effectDataField.offset);
	}
	
	public ResizingBuffer getEffectDataSlice() {
		return getBuffer().slice(effectDataField.offset);
	}
	
}
