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
 * A header storing incoming event specific meta-data for processing.
 * 
 * @author Adam Roughton
 * @see EventHeader
 */
public class IncomingEventHeader extends EventHeader {
	
	public IncomingEventHeader(final int startOffset, final int segmentCount) {
		this(startOffset, segmentCount, 0, 0);
	}
	
	protected IncomingEventHeader(final int startOffset, final int segmentCount, final int additionalLength, final int additionalFlagCount) {
		super(startOffset, segmentCount, additionalLength + 4, additionalFlagCount);
	}
	
	public final int getSocketId(byte[] event) {
		return MessageBytesUtil.readInt(event, getAdditionalOffset());
	}
	
	public final void setSocketId(byte[] event, int socketId) {
		MessageBytesUtil.writeInt(event, getAdditionalOffset(), socketId);
	}
	
}
