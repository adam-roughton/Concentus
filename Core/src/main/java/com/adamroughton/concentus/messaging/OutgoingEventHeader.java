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
	private static final int SENT_TIME_LENGTH = 8;
	private static final int TARGET_SOCKET_ID_LENGTH = 4;
	private static final int ADDITIONAL_LENGTH = NEXT_SEGMENT_LENGTH + SENT_TIME_LENGTH + TARGET_SOCKET_ID_LENGTH;
	private static final int FLAG_COUNT = 2;
	
	private final int _nextSegmentOffset;
	private final int _sentTimeOffset;
	private final int _targetSocketIdOffset;
	private final int _isPartiallySentFlagIndex;
	private final int _isReliableFlagIndex;
	
	public OutgoingEventHeader(final int startOffset, final int segmentCount) {
		this(startOffset, segmentCount, 0, 0);
	}
	
	protected OutgoingEventHeader(final int startOffset, final int segmentCount, final int additionalLength, final int additionalFlagCount) {
		super(startOffset, segmentCount, additionalLength + ADDITIONAL_LENGTH, additionalFlagCount + FLAG_COUNT);
		_nextSegmentOffset = super.getAdditionalOffset();
		_sentTimeOffset = _nextSegmentOffset + NEXT_SEGMENT_LENGTH;
		_targetSocketIdOffset = _sentTimeOffset + SENT_TIME_LENGTH;
		_isPartiallySentFlagIndex = super.getAdditionalFlagsStartIndex();
		_isReliableFlagIndex = _isPartiallySentFlagIndex + 1;
	}
	
	@Override
	protected int getAdditionalFlagsStartIndex() {
		return super.getAdditionalFlagsStartIndex() + FLAG_COUNT;
	}
	
	@Override
	protected int getAdditionalOffset() {
		return super.getAdditionalOffset() + ADDITIONAL_LENGTH;
	}

	public final boolean isPartiallySent(byte[] event) {
		return getFlag(event, _isPartiallySentFlagIndex);
	}
	
	public final void setIsPartiallySent(byte[] event, boolean isPartiallySent) {
		setFlag(event, _isPartiallySentFlagIndex, isPartiallySent);
	}
	
	public final boolean isReliable(byte[] event) {
		return getFlag(event, _isReliableFlagIndex);
	}
	
	public final void setIsReliable(byte[] event, boolean isReliable) {
		setFlag(event, _isReliableFlagIndex, isReliable);
	}
	
	public final int getNextSegmentToSend(byte[] event) {
		return MessageBytesUtil.readInt(event, _nextSegmentOffset);
	}
	
	public final void setNextSegmentToSend(byte[] event, int segmentIndex) {
		MessageBytesUtil.writeInt(event, _nextSegmentOffset, segmentIndex);
	}
	
	public final int getTargetSocketId(byte[] event) {
		return MessageBytesUtil.readInt(event, _targetSocketIdOffset);
	}
	
	public final void setTargetSocketId(byte[] event, int socketId) {
		MessageBytesUtil.writeInt(event, _targetSocketIdOffset, socketId);
	}
	
	public final long getSentTime(byte[] event) {
		return MessageBytesUtil.readLong(event, _sentTimeOffset);
	}
	
	public final void setSentTime(byte[] event, long sentTime) {
		MessageBytesUtil.writeLong(event, _sentTimeOffset, sentTime);
	}
	
}
