package com.adamroughton.consentus.messaging.events;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public class StateUpdateInfoEvent extends ByteArrayBackedEvent {

	private final static int UPDATE_ID_OFFSET = 0;
	private final static int ENTRY_COUNT_OFFSET = 8;
	private final static int ENTRY_START_OFFSET = 12;
	private final static int ENTRY_SIZE = 12;

	public StateUpdateInfoEvent() {
		super(EventType.STATE_INFO.getId());
	}
	
	public long getUpdateId() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET));
	}
	
	public void setUpdateId(long updateId) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(UPDATE_ID_OFFSET), updateId);
	}
	
	public int getEntryCount() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(ENTRY_COUNT_OFFSET));
	}
	
	public void setEntryCount(int entryCount) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(ENTRY_COUNT_OFFSET), entryCount);
	}
	
	public ClientHandlerEntry getHandlerEntry(int index) {
		int clientHandlerId = MessageBytesUtil.readInt(getBackingArray(), getOffset(getEntryOffset(index)));
		long highestSeq = MessageBytesUtil.readLong(getBackingArray(), getOffset(getEntryOffset(index) + 4));
		return new ClientHandlerEntry(clientHandlerId, highestSeq);
	}
	
	public void setHandlerEntry(int index, ClientHandlerEntry entry) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(getEntryOffset(index)), entry.getClientHandlerId());
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(getEntryOffset(index) + 4), entry.getHighestHandlerSeq());
	}

	private int getEntryOffset(int index) {
		return ENTRY_START_OFFSET + ENTRY_SIZE * index;
	}
	
	/**
	 * Helper method for determining the number of entries that can
	 * be placed in this event given the underlying byte array size.
	 * {@link #setBackingArray(byte[], int)} should be called with the 
	 * intended backing array before invoking this method.
	 * @return the number of entries that can be written into this event
	 */
	public int getMaximumEntries() {
		int availableBytes = getAvailableSize() - 12;
		return availableBytes / ENTRY_SIZE;
	}
	
	/**
	 * Gets the Client Handler ID of the first
	 * entry. This is useful for determining the range
	 * of IDs contained within this event.
	 * @return the ID of the first Client Handler, or {@code -1} if no entries
	 * are contained within this event.
	 */
	public int getStartingClientHandlerId() {
		if (getEntryCount() > 0) {
			return getClientHandlerIdAtIndex(0);
		} else {
			return -1;
		}
	}
	
	public int getClientHandlerIdAtIndex(int index) {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(getEntryOffset(index)));
	}
	
	public long getHighestSequenceAtIndex(int index) {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(getEntryOffset(index) + 4));
	}
	
	/**
	 * Gets the Client Handler ID of the last
	 * entry. This is useful for determining the range
	 * of IDs contained within this event.
	 * @return the ID of the last Client Handler, or {@code -1} if no entries
	 * are contained within this event.
	 */
	public int getLastClientHandlerId() {
		int entryCount = getEntryCount();
		if (entryCount > 0) {
			return getClientHandlerIdAtIndex(entryCount - 1);
		} else {
			return -1;
		}
	}
	
}
