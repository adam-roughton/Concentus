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
package com.adamroughton.consentus.messaging.events;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.model.ClientId;

public class ConnectResponseEvent extends ByteArrayBackedEvent {
	
	private static final int RES_CODE_OFFSET = 0;
	private static final int CLIENT_ID_OFFSET = 4;
	
	public static final int RES_OK = 0;
	public static final int RES_ERROR = 1;

	public ConnectResponseEvent() {
		super(EventType.CONNECT_RES.getId());
	}
	
	public int getResponseCode() {
		return MessageBytesUtil.readInt(getBackingArray(), getOffset(RES_CODE_OFFSET));
	}
	
	public void setResponseCode(final int responseCode) {
		MessageBytesUtil.writeInt(getBackingArray(), getOffset(RES_CODE_OFFSET), responseCode);
	}
	
	public ClientId getClientId() {
		return MessageBytesUtil.readClientId(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}

	public void setClientId(ClientId clientId) {
		MessageBytesUtil.writeClientId(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientId);
	}
	
	public void setClientId(long clientIdBits) {
		MessageBytesUtil.writeLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET), clientIdBits);
	}
	
	public long getClientIdBits() {
		return MessageBytesUtil.readLong(getBackingArray(), getOffset(CLIENT_ID_OFFSET));
	}
	
}
