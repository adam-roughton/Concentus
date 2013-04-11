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
 * An outgoing event header that additionally specifies the identity of the target
 * socket for sending. This allows more than one socket to share an outgoing queue.
 * The target socket ID is stored after 
 * 
 * @author Adam Roughton
 * @see OutgoingEventHeader
 */
public class MultiSocketOutgoingEventHeader extends OutgoingEventHeader {
	
	private static final int TARGET_SOCKET_ID_LENGTH = 4;
	
	private final int _targetSocketIdOffset;
	
	public MultiSocketOutgoingEventHeader(final int startOffset, final int segmentCount) {
		this(startOffset, segmentCount, 0, 0);
	}
	
	protected MultiSocketOutgoingEventHeader(final int startOffset, final int segmentCount, final int additionalLength, final int additionalFlagCount) {
		super(startOffset, segmentCount, additionalLength + TARGET_SOCKET_ID_LENGTH, additionalFlagCount);
		_targetSocketIdOffset = super.getAdditionalOffset();
	}
	
	@Override
	protected int getAdditionalOffset() {
		return super.getAdditionalOffset() + TARGET_SOCKET_ID_LENGTH;
	}

	public final int getTargetSocketId(byte[] event) {
		return MessageBytesUtil.readInt(event, _targetSocketIdOffset);
	}
	
	public final void setTargetSocketId(byte[] event, int socketId) {
		MessageBytesUtil.writeInt(event, _targetSocketIdOffset, socketId);
	}
	
}
