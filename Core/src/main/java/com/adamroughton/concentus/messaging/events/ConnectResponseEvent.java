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

import com.adamroughton.concentus.model.ClientId;
import static com.adamroughton.concentus.messaging.ResizingBuffer.*;

public final class ConnectResponseEvent extends BufferBackedObject {
	
	private final Field callbackBitsField = super.getBaseField().then(LONG_SIZE);
	private final Field resCodeField = callbackBitsField.then(INT_SIZE);
	private final Field clientIdField = resCodeField.then(LONG_SIZE)
			.resolveOffsets();
	
	public static final int RES_OK = 0;
	public static final int RES_ERROR = 1;

	public ConnectResponseEvent() {
		super(EventType.CONNECT_RES.getId());
	}
	
	/**
	 * Gets the data that associates the response to the original connect request within the domain
	 * of the sender: i.e. this does not globally match responses to requests; if a sender reuses 
	 * callback bits for another connect event, it is up to the sender to distinguish the first response
	 * from the second. 
	 * @return the data stored in the connect request event by the sender
	 * @see ClientConnectEvent#setCallbackBits(long)
	 */
	public long getCallbackBits() {
		return getBuffer().readLong(callbackBitsField.offset);
	}
	
	/**
	 * @see ConnectResponseEvent#getCallbackBits()
	 * @param callbackBits the data stored in the connect request event by the sender 
	 */
	public void setCallbackBits(long callbackBits) {
		getBuffer().writeLong(callbackBitsField.offset, callbackBits);
	}
	
	public int getResponseCode() {
		return getBuffer().readInt(resCodeField.offset);
	}
	
	public final void setResponseCode(final int responseCode) {
		getBuffer().writeInt(resCodeField.offset, responseCode);
	}
	
	public final ClientId getClientId() {
		return getBuffer().readClientId(clientIdField.offset);
	}

	public final void setClientId(ClientId clientId) {
		getBuffer().writeClientId(clientIdField.offset, clientId);
	}
	
	public final void setClientId(long clientIdBits) {
		getBuffer().writeLong(clientIdField.offset, clientIdBits);
	}
	
	public final long getClientIdBits() {
		return getBuffer().readLong(clientIdField.offset);
	}
	
}
