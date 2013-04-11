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
package com.adamroughton.concentus.messaging;

/**
 * A header storing outgoing event specific meta-data for processing.
 * 
 * @author Adam Roughton
 * @see EventHeader
 */
public class OutgoingEventHeader extends EventHeader {
	
	private static final int NEXT_SEGMENT_LENGTH = 4;
	private static final int FLAG_COUNT = 1;
	
	private final int _nextSegmentOffset;
	private final int _isPartiallySentFlagIndex;
	
	public OutgoingEventHeader(final int startOffset, final int segmentCount) {
		this(startOffset, segmentCount, 0, 0);
	}
	
	protected OutgoingEventHeader(final int startOffset, final int segmentCount, final int additionalLength, final int additionalFlagCount) {
		super(startOffset, segmentCount, additionalLength + NEXT_SEGMENT_LENGTH, additionalFlagCount + FLAG_COUNT);
		_nextSegmentOffset = super.getAdditionalOffset();
		_isPartiallySentFlagIndex = super.getAdditionalFlagsStartIndex();
	}
	
	@Override
	protected int getAdditionalFlagsStartIndex() {
		return super.getAdditionalFlagsStartIndex() + FLAG_COUNT;
	}
	
	@Override
	protected int getAdditionalOffset() {
		return super.getAdditionalOffset() + NEXT_SEGMENT_LENGTH;
	}

	public final boolean isPartiallySent(byte[] event) {
		return getFlag(event, _isPartiallySentFlagIndex);
	}
	
	public final void setIsPartiallySent(byte[] event, boolean isPartiallySent) {
		setFlag(event, _isPartiallySentFlagIndex, isPartiallySent);
	}
	
	public final int getNextSegmentToSend(byte[] event) {
		return MessageBytesUtil.readInt(event, _nextSegmentOffset);
	}
	
	public final void setNextSegmentToSend(byte[] event, int segmentIndex) {
		MessageBytesUtil.writeInt(event, _nextSegmentOffset, segmentIndex);
	}
	
}