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

public class ClientConnectEvent extends ByteArrayBackedEvent {
	
	private static final int EVENT_SIZE = 0;
	/**
	 * Brushing over security details for now
	 */
	//private static final int AUTH_TOKEN_OFFSET = 0;

	public ClientConnectEvent() {
		super(EventType.CLIENT_CONNECT.getId(), EVENT_SIZE);
	}
	
}
