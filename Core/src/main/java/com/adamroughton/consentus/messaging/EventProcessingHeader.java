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
package com.adamroughton.consentus.messaging;

/**
 * A header is reserved on the byte arrays that flow from event receiver to processor, and processor
 * to sender; for communicating event validity, and other meta data about the event. This class allows
 * processors, sender, and receiver components to coordinate over the size and properties of this header.
 * 
 * @author Adam Roughton
 *
 */
public final class EventProcessingHeader {

	private final int _startOffset;
	private final int _length;
	
	public EventProcessingHeader(final int startOffset, 
			final int length) {
		if (startOffset < 0) throw new IllegalArgumentException("The start offset must be not be negative.");
		_startOffset = startOffset;
		if (length < 0) throw new IllegalArgumentException("The length must be not be negative.");
		_length = length;
	}
	
	public boolean isValid(byte[] event) {
		return MessageBytesUtil.readFlagFromByte(event, _startOffset, 0);
	}
	
	public void setIsValid(boolean isValid, byte[] event) {
		MessageBytesUtil.writeFlagToByte(event, _startOffset, 0, isValid);
	}
	
	public int getSocketId(byte[] event) {
		return MessageBytesUtil.read4BitUInt(event, 0, 4);
	}
	
	public void setSocketId(int socketId, byte[] event) {
		MessageBytesUtil.write4BitUInt(event, 0, 4, socketId);
	}
	
	public int getEventOffset() {
		return _startOffset + _length;
	}
	
	public int getLength() {
		return _length;
	}
	
}
