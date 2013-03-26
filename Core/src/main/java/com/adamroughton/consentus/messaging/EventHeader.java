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
 * A header is reserved on the allocated event memory that flow from event receiver to processor, and processor
 * to sender; for communicating event validity, and other meta data about the event. This class serves as the 
 * base class for all event headers.
 * 
 * @author Adam Roughton
 *
 */
public abstract class EventHeader {

	private final int _segmentCount;
	private final int _startOffset;
	private final int _length;
	private final int _segmentsStartOffset;
	private final int _flagFieldLength;
	private final int _additionalOffset;
	
	public EventHeader(final int startOffset, 
			final int segmentCount,
			final int additionalLength,
			final int additionalFlagCount) {
		if (startOffset < 0) throw new IllegalArgumentException("The start offset must be not be negative.");
		_startOffset = startOffset;
		if (segmentCount <= 0) throw new IllegalArgumentException("The event must have at least one segment.");
		_segmentCount = segmentCount;
		if (additionalLength < 0) throw new IllegalArgumentException("The additional length must be not be negative.");
		
		if (additionalFlagCount < 0) throw new IllegalArgumentException("The additional flag count must be not be negative.");
		int flagCount = additionalFlagCount + 1;
		_flagFieldLength = (flagCount / 8) + (flagCount % 8 != 0? 1 : 0);
		_segmentsStartOffset = _flagFieldLength;
		
		/*
		 * We use the first byte for flags, (segment count) * 4 bytes for segment offset/length pairs (each pair is one
		 * integer, or 2 16bit numbers), and finally allocate the additional bytes used by classes that subclass this
		 * class.
		 */
		int baseLength = _segmentsStartOffset + _segmentCount * 4;
		_additionalOffset = baseLength;
		_length = baseLength + additionalLength;
	}
	
	protected final int getAdditionalOffset() {
		return _additionalOffset;
	}
	
	protected int getAdditionalFlagsStartIndex() {
		return 1;
	}
	
	protected final boolean getFlag(byte[] event, int flagIndex) {
		return MessageBytesUtil.readFlag(event, 0, _flagFieldLength, flagIndex);
	}
	
	protected final void setFlag(byte[] event, int flagIndex, boolean isRaised) {
		MessageBytesUtil.writeFlag(event, 0, _flagFieldLength, flagIndex, isRaised);
	}
	
	public final int getSegmentCount() {
		return _segmentCount;
	}
	
	public final boolean isValid(byte[] event) {
		return getFlag(event, 0);
	}
	
	public final void setIsValid(byte[] event, boolean isValid) {
		setFlag(event, 0, isValid);
	}
	
	public final int getSegmentMetaData(byte[] event, int segmentIndex) {
		return MessageBytesUtil.readInt(event, _segmentsStartOffset + segmentIndex * 4);
	}
	
	public final void setSegmentMetaData(byte[] event, int segmentIndex, int segmentMetaData) {
		MessageBytesUtil.writeInt(event, _segmentsStartOffset + segmentIndex * 4, segmentMetaData);
	}
	
	public final void setSegmentMetaData(byte[] event, int segmentIndex, int segmentOffset, int segmentLength) {
		setSegmentMetaData(event, segmentIndex, createSegmentMetaData(segmentOffset, segmentLength));
	}
	
	public static int getSegmentOffset(int segmentMetaData) {
		return segmentMetaData & 0xFFFF;
	}
	
	public static int getSegmentLength(int segmentMetaData) {
		return (segmentMetaData >> 16) & 0xFFFF;
	}
	
	public static int createSegmentMetaData(int segmentOffset, int segmentLength) {
		int segmentMetaData = segmentOffset & 0xFFFF;
		segmentMetaData |= ((segmentLength & 0xFFFF) << 16);
		return segmentMetaData;
	}
	
	public final int getEventOffset() {
		return _startOffset + _length;
	}
	
	public final int getLength() {
		return _length;
	}
	
}
