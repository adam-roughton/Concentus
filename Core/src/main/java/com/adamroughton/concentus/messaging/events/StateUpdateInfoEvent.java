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

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public class StateUpdateInfoEvent extends ByteArrayBackedEvent {

	private final static int UPDATE_ID_OFFSET = 0;
	private final static int ENTRY_COUNT_OFFSET = 8;
	private final static int ENTRY_START_OFFSET = 12;
	private final static int ENTRY_SIZE = 12;
	private static final int BASE_SIZE = ENTRY_START_OFFSET;

	public StateUpdateInfoEvent() {
		super(EventType.STATE_INFO.getId());
	}
	
	public final long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET));
	}
	
	public final void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET), updateId);
	}
	
	public final int getEntryCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(ENTRY_COUNT_OFFSET));
	}
	
	public final void setEntryCount(int entryCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(ENTRY_COUNT_OFFSET), entryCount);
		setEventSize(BASE_SIZE + entryCount * ENTRY_SIZE);
	}
	
	public final ClientHandlerEntry getHandlerEntry(int index) {
		int clientHandlerId = MessageBytesUtil.readInt(getBackingArray(), getOffset(getEntryOffset(index)));
		long highestSeq = MessageBytesUtil.readLong(getBackingArray(), getOffset(getEntryOffset(index) + 4));
		return new ClientHandlerEntry(clientHandlerId, highestSeq);
	}
	
	public final void setHandlerEntry(int index, ClientHandlerEntry entry) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(getEntryOffset(index)), entry.getClientHandlerId());
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(getEntryOffset(index) + 4), entry.getHighestHandlerSeq());
	}

	/**
	 * Gets the offset of the entry at index {@code index} relative to the {@link StateUpdateInfoEvent#ENTRY_START_OFFSET}.
	 * This must be transformed by {@link ByteArrayBackedEvent#getOffset(int)} to get the offset relative to the
	 * start of the event.
	 * @param index the index of the entry
	 * @return the offset relative to {@link StateUpdateInfoEvent#ENTRY_START_OFFSET}
	 */
	private final int getEntryOffset(int index) {
		return ENTRY_START_OFFSET + ENTRY_SIZE * index;
	}
	
	/**
	 * Helper method for determining the number of entries that can
	 * be placed in this event given the underlying byte array size.
	 * {@link #setBackingArray(byte[], int)} should be called with the 
	 * intended backing array before invoking this method.
	 * @return the number of entries that can be written into this event
	 */
	public final int getMaximumEntries() {
		int availableBytes = getAvailableSize() - getOffset(ENTRY_START_OFFSET);
		return availableBytes / ENTRY_SIZE;
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
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(getEntryOffset(index)));
	}
	
	public final long getHighestSequenceAtIndex(int index) {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(getEntryOffset(index) + 4));
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
