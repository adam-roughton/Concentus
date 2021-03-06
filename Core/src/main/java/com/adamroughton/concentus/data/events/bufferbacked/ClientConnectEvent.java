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
package com.adamroughton.concentus.data.events.bufferbacked;
import static com.adamroughton.concentus.data.ResizingBuffer.*;

import com.adamroughton.concentus.data.BufferBackedObject;
import com.adamroughton.concentus.data.DataType;

public final class ClientConnectEvent extends BufferBackedObject {
	
	private final Field callbackBitsField = super.getBaseField().then(LONG_SIZE)
			.resolveOffsets();
	
	/**
	 * Brushing over security details for now
	 */
	//private static final int AUTH_TOKEN_OFFSET = 0;

	public ClientConnectEvent() {
		super(DataType.CLIENT_CONNECT_EVENT);
	}
	
	@Override
	protected Field getBaseField() {
		return callbackBitsField;
	}
	
	/**
	 * @see ClientConnectEvent#setCallbackBits(long)
	 * @return the bits stored in the callback bits field
	 */
	public final long getCallbackBits() {
		return getBuffer().readLong(callbackBitsField.offset);
	}
	
	/**
	 * Up to 64 bits of data can be stored in the connect event for matching a request
	 * to a subsequent response. The response for a connect request will always contain
	 * the callback bits passed through the request.
	 * @param callbackBits any data that allows the sender to match the response to the request.
	 * The choice of data is at the sole discretion of the sender, with no side effects if the
	 * same bits are reused for subsequent requests.
	 */
	public final void setCallbackBits(long callbackBits) {
		getBuffer().writeLong(callbackBitsField.offset, callbackBits);
	}
}
