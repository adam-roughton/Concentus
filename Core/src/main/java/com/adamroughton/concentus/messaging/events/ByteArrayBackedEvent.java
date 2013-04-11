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

import java.nio.ByteBuffer;

import com.adamroughton.concentus.messaging.MessageBytesUtil;

public abstract class ByteArrayBackedEvent {

	private final int _id;
	private final int _defaultEventSize;
	
	private byte[] _backingArray;
	private int _offset;
	private int _eventSize;
	
	protected ByteArrayBackedEvent(int typeId) {
		this(typeId, 0);
	}
	
	/**
	 * Creates a new instance with the given typeId. The default event
	 * size allows implementors to avoid calling {@link ByteArrayBackedEvent#setEventSize(int)}
	 * each time they set a new backing array.
	 * @param typeId
	 * @param defaultEventSize the default size of the event <b><u>not including the typeId</u></b>. This class
	 * adds any necessary header additions to the size.
	 */
	protected ByteArrayBackedEvent(int typeId, int defaultEventSize) {
		_id = typeId;
		_defaultEventSize = defaultEventSize + 4;
	}
	
	public final byte[] getBackingArray() {
		return _backingArray;
	}
	
	public final void setBackingArray(byte[] backingArray, int offset) {
		_backingArray = backingArray;
		_eventSize = _defaultEventSize;
		_offset = offset;
		MessageBytesUtil.writeInt(_backingArray, offset, _id);
		_offset += 4;
	}
	
	public final int getEventTypeId() {
		return _id;
	}
	
	protected final void setEventSize(int size) {
		_eventSize = size + 4;
	}
	
	public final int getEventSize() {
		return _eventSize;
	}
	
	/**
	 * Calculates the absolute offset of the field on the backing byte array.
	 * @param internalFieldOffset the byte offset of the field relative to the other fields in the event
	 * @return the absolute offset
	 */
	protected final int getOffset(int internalFieldOffset) {
		return _offset + internalFieldOffset;
	}
	
	public final void releaseBackingArray() {
		_backingArray = null;
		_offset = 0;
		_eventSize = 0;
	}
	
	/**
	 * Helper method for determining the number of bytes that can
	 * be written to this event by the super class given the underlying 
	 * byte array size.
	 * {@link #setBackingArray(byte[], int)} should be called with the 
	 * intended backing array before invoking this method.
	 * @return the number of bytes that can be written into this event
	 */
	protected final int getAvailableSize() {
		return _backingArray.length - 4;
	}
	
	public static int getUsedLength(int startPos, ByteBuffer buffer) {
		return buffer.position() - startPos;
	}

}
