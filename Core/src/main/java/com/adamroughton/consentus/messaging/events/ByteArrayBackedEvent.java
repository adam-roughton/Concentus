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
	
	public int getEventTypeId() {
		return _id;
	}
	
	/**
	 * Calculates the absolute offset of the field on the backing byte array.
	 * @param internalFieldOffset the byte offset of the field relative to the other fields in the event
	 * @return the absolute offset
	 */
	protected int getOffset(int internalFieldOffset) {
		return _offset + internalFieldOffset;
	}
	
	public void releaseBackingArray() {
		_backingArray = null;
		_offset = 0;
	}
	
	/**
	 * Helper method for determining the number of bytes that can
	 * be written to this event by the super class given the underlying 
	 * byte array size.
	 * {@link #setBackingArray(byte[], int)} should be called with the 
	 * intended backing array before invoking this method.
	 * @return the number of bytes that can be written into this event
	 */
	protected int getAvailableSize() {
		return _backingArray.length - (_writeId? 4 : 0);
	}
}
