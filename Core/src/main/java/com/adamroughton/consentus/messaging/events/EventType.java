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

public enum EventType {
	CLIENT_INPUT(1, ClientInputEvent.class),
	STATE_INPUT(2, StateInputEvent.class),
	STATE_UPDATE(3, StateUpdateEvent.class),
	STATE_METRIC(4, StateMetricEvent.class),
	STATE_INFO(5, StateUpdateInfoEvent.class),
	CLIENT_CONNECT(6, ClientConnectEvent.class),
	CONNECT_RES(7, ConnectResponseEvent.class)
	;
	private final int _id;
	private final Class<?> _clazz;
	
	private EventType(final int id, final Class<?> clazz) {
		_id = id;
		_clazz = clazz;
	}
	
	public int getId() {
		return _id;
	}
	
	public Class<?> getEventClass() {
		return _clazz;
	}
}