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

import com.adamroughton.concentus.data.ResizingBuffer;

/**
 * A header storing incoming event specific meta-data for processing.
 * 
 * @author Adam Roughton
 * @see EventHeader
 */
public class IncomingEventHeader extends EventHeader {
	
	private final static int SOCKET_ID_LENGTH = 4;
	private final static int ADDITIONAL_LENGTH = SOCKET_ID_LENGTH;
	private final static int FLAG_COUNT = 0;
	
	private final int _socketIdOffset;
	
	public IncomingEventHeader(final int startOffset, final int segmentCount) {
		this(startOffset, segmentCount, 0, 0);
	}
	
	protected IncomingEventHeader(final int startOffset, final int segmentCount, final int additionalLength, final int additionalFlagCount) {
		super(startOffset, segmentCount, additionalLength + ADDITIONAL_LENGTH, additionalFlagCount + FLAG_COUNT);
		_socketIdOffset = super.getAdditionalOffset();
	}
	
	@Override
	protected int getAdditionalFlagsStartIndex() {
		return super.getAdditionalFlagsStartIndex() + FLAG_COUNT;
	}
	
	@Override
	protected int getAdditionalOffset() {
		return super.getAdditionalOffset() + ADDITIONAL_LENGTH;
	}
	
	public final int getSocketId(ResizingBuffer event) {
		return event.readInt(_socketIdOffset);
	}
	
	public final void setSocketId(ResizingBuffer event, int socketId) {
		event.writeInt(_socketIdOffset, socketId);
	}
	
	/**
	 * Checks the source of the event to determine if this message
	 * was enqueued internally rather than from a messenger.
	 * @param event
	 * @return true if this message was enqueued internally, false otherwise
	 */
	public final boolean isFromInternal(ResizingBuffer event) {
		return getSocketId(event) == -1;
	}
	
	public final void setFromInternal(ResizingBuffer event) {
		setSocketId(event, -1);
	}
	
}
