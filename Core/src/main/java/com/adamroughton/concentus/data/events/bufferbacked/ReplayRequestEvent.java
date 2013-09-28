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
import com.adamroughton.concentus.data.DataType;

public final class ReplayRequestEvent extends BufferBackedObject {
	
	private final Field startSeqField = super.getBaseField().then(LONG_SIZE);
	private final Field countField = startSeqField.then(INT_SIZE)
			.resolveOffsets();
	
	public ReplayRequestEvent() {
		super(DataType.REPLAY_REQUEST_EVENT);
	}
	
	public long getStartSequence() {
		return getBuffer().readLong(startSeqField.offset);
	}
	
	public void setStartSequence(long startSequence) {
		getBuffer().writeLong(startSeqField.offset, startSequence);
	}
	
	public int getCount() {
		return getBuffer().readInt(countField.offset);
	}
	
	public void setCount(int count) {
		getBuffer().writeInt(countField.offset, count);
	}
	
}
