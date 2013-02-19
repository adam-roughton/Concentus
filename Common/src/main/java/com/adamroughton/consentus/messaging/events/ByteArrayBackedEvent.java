package com.adamroughton.consentus.messaging.events;

import com.adamroughton.consentus.messaging.MessageBytesUtil;

public abstract class ByteArrayBackedEvent {

	private final boolean _writeId;
	private final int _id;
	
	private byte[] _backingArray;
	private int _offset;
	
	public ByteArrayBackedEvent() {
		_writeId = false;
		_id = 0;
	}
	
	public ByteArrayBackedEvent(int typeId) {
		_writeId = true;
		_id = typeId;
	}
	
	public byte[] getBackingArray() {
		return _backingArray;
	}
	
	public void setBackingArray(byte[] backingArray, int offset) {
		_backingArray = backingArray;
		_offset = offset;
		if (_writeId) {
			MessageBytesUtil.writeInt(_backingArray, offset, _id);
			_offset += 4;
		}
	}
	
	/**
	 * Calculates the absolute offset of the field on the backing byte array.
	 * @param internalFieldOffset the byte offset of the field relative to the other fields in the event
	 * @return the absolute offset
	 */
	protected int getOffset(int internalFieldOffset) {
		return _offset + internalFieldOffset;
	}
	
	public void clear() {
		_backingArray = null;
		_offset = 0;
	}
}
