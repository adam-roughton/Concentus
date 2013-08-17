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

public final class StateUpdateInfoEvent extends BufferBackedObject {

	private final Field updateIdField = super.getBaseField().then(LONG_SIZE);
	private final Field entryCountField = updateIdField.then(INT_SIZE);
	private final Field entryContentField = entryCountField.thenVariableLength()
			.resolveOffsets();
	private final static int ENTRY_SIZE = INT_SIZE + LONG_SIZE;

	public StateUpdateInfoEvent() {
		super(EventType.STATE_INFO.getId());
	}
	
	public long getUpdateId() {
		return getBuffer().readLong(updateIdField.offset);
	}
	
	public void setUpdateId(long updateId) {
		getBuffer().writeLong(updateIdField.offset, updateId);
	}
	
	public int getEntryCount() {
		return getBuffer().readInt(entryCountField.offset);
	}
	
	public void setEntryCount(int entryCount) {
		getBuffer().writeInt(entryCountField.offset, entryCount);
	}
	
	public final ClientHandlerEntry getHandlerEntry(int index) {
		int clientHandlerId = getBuffer().readInt(getEntryOffset(index));
		long highestSeq = getBuffer().readLong(getEntryOffset(index) + INT_SIZE);
		return new ClientHandlerEntry(clientHandlerId, highestSeq);
	}
	
	public final void setHandlerEntry(int index, ClientHandlerEntry entry) {
		getBuffer().writeInt(getEntryOffset(index), entry.getClientHandlerId());
		getBuffer().writeLong(getEntryOffset(index) + INT_SIZE, entry.getHighestHandlerSeq());
	}

	/**
	 * Gets the offset of the entry at index {@code index} relative to the {@link StateUpdateInfoEvent#entryContentField}.
	 * @param index the index of the entry
	 * @return the offset relative to {@link StateUpdateInfoEvent#entryContentField}
	 */
	private final int getEntryOffset(int index) {
		return entryContentField.offset + ENTRY_SIZE * index;
	}
	
	/**
	 * Gets the Client Handler ID of the first
	 * entry. This is useful for determining the range
	 * of IDs contained within this event.
	 * @return the ID of the first Client Handler, or {@code -1} if no entries
	 * are contained within this event.
	 */
	public final int getStartingClientHandlerId() {
		if (getEntryCount() > 0) {
			return getClientHandlerIdAtIndex(0);
		} else {
			return -1;
		}
	}
	
	public final int getClientHandlerIdAtIndex(int index) {
		return getBuffer().readInt(getEntryOffset(index));
	}
	
	public final long getHighestSequenceAtIndex(int index) {
		return getBuffer().readLong(getEntryOffset(index) + INT_SIZE);
	}
	
	/**
	 * Gets the Client Handler ID of the last
	 * entry. This is useful for determining the range
	 * of IDs contained within this event.
	 * @return the ID of the last Client Handler, or {@code -1} if no entries
	 * are contained within this event.
	 */
	public final int getLastClientHandlerId() {
		int entryCount = getEntryCount();
		if (entryCount > 0) {
			return getClientHandlerIdAtIndex(entryCount - 1);
		} else {
			return -1;
		}
	}
	
}
