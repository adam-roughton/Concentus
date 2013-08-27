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

	private final Field clientIdField = super.getBaseField().then(LONG_SIZE);
	private final Field contentField = clientIdField.thenVariableLength()
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
