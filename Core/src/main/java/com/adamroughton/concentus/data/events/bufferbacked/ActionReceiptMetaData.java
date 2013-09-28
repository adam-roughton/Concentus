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
import com.adamroughton.concentus.data.model.ClientId;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

/**
 * Data object internal to Action Collectors that allows a minimal amount
 * of information to be stored for an action receipt (clientId, actionId, 
 * start time, and the collective variable IDs affected). This information 
 * can be used to generate a fresh receipt with the most up to date 
 * information when action receipts are lost in transmission.
 * @author Adam Roughton
 *
 */
public final class ActionReceiptMetaData extends BufferBackedObject {

	private final Field clientIdField = super.getBaseField().then(LONG_SIZE);
	private final Field actionIdField = clientIdField.then(LONG_SIZE);
	private final Field startTimeField = actionIdField.then(LONG_SIZE);
	private final Field varIdCountField = startTimeField.then(SHORT_SIZE);
	private final Field varIdsOffset = varIdCountField.thenVariableLength()
			.resolveOffsets();
	
	public ActionReceiptMetaData() {
		super(DataType.ACTION_RECEIPT_META_DATA);
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
	
	public int getVarIdCount() {
		return getBuffer().readShort(varIdCountField.offset);
	}
	
	public void setVarIdCount(int count) {
		getBuffer().writeShort(varIdCountField.offset, (short) count);
	}
	
	public ResizingBuffer getVarIdsSlice() {
		return getBuffer().slice(varIdsOffset.offset);
	}
	
}
