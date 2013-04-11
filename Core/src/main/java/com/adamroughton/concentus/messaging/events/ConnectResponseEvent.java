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

import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.model.ClientId;

public class ConnectResponseEvent extends ByteArrayBackedEvent {
	
	private static final int CALLBACK_BITS_OFFSET = 0;
	private static final int RES_CODE_OFFSET = 8;
	private static final int CLIENT_ID_OFFSET = 12;
	private static final int EVENT_SIZE = CLIENT_ID_OFFSET + 8;
	
	public static final int RES_OK = 0;
	public static final int RES_ERROR = 1;

	public ConnectResponseEvent() {
		super(EventType.CONNECT_RES.getId(), EVENT_SIZE);
	}
	
	/**
	 * Gets the data that associates the response to the original connect request within the domain
	 * of the sender: i.e. this does not globally match responses to requests; if a sender reuses 
	 * callback bits for another connect event, it is up to the sender to distinguish the first response
	 * from the second. 
	 * @return the data stored in the connect request event by the sender
	 * @see ClientConnectEvent#setCallbackBits(long)
	 */
	public final long getCallbackBits() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(CALLBACK_BITS_OFFSET));
	}
	
	/**
	 * @see ConnectResponseEvent#getCallbackBits()
	 * @param callbackBits the data stored in the connect request event by the sender 
	 */
	public final void setCallbackBits(long callbackBits) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CALLBACK_BITS_OFFSET), callbackBits);
	}
	
	public final int getResponseCode() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(RES_CODE_OFFSET));
	}
	
	public final void setResponseCode(final int responseCode) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(RES_CODE_OFFSET), responseCode);
	}
	
	public final ClientId getClientId() {
		return MessageBytesUtil.readClientId(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}

	public final void setClientId(ClientId clientId) {
		MessageBytesUtil.writeClientId(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientId);
	}
	
	public final void setClientId(long clientIdBits) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientIdBits);
	}
	
	public final long getClientIdBits() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}
	
}
