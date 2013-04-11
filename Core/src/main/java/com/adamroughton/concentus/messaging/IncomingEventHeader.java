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
 * A header storing incoming event specific meta-data for processing.
 * 
 * @author Adam Roughton
 * @see EventHeader
 */
public class IncomingEventHeader extends EventHeader {
	
	private final static int SOCKET_ID_LENGTH = 4;
	
	private final int _socketIdOffset;
	
	public IncomingEventHeader(final int startOffset, final int segmentCount) {
		this(startOffset, segmentCount, 0, 0);
	}
	
	protected IncomingEventHeader(final int startOffset, final int segmentCount, final int additionalLength, final int additionalFlagCount) {
		super(startOffset, segmentCount, additionalLength + SOCKET_ID_LENGTH, additionalFlagCount);
		_socketIdOffset = super.getAdditionalOffset();
	}
	
	@Override
	protected int getAdditionalOffset() {
		return super.getAdditionalOffset() + SOCKET_ID_LENGTH;
	}

	public final int getSocketId(byte[] event) {
		return MessageBytesUtil.readInt(event, _socketIdOffset);
	}
	
	public final void setSocketId(byte[] event, int socketId) {
		MessageBytesUtil.writeInt(event, _socketIdOffset, socketId);
	}
	
}