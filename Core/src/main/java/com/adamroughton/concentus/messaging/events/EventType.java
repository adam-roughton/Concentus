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

public enum EventType {
	NULL(0, null),
	CLIENT_INPUT(1, ClientInputEvent.class),
	ACTION_RECEIPT(2, ActionReceiptEvent.class),
	CLIENT_UPDATE(3, ClientUpdateEvent.class),
	STATE_INPUT(4, StateInputEvent.class),
	STATE_UPDATE(5, StateUpdateEvent.class),
	CLIENT_CONNECT(6, ClientConnectEvent.class),
	CONNECT_RES(7, ConnectResponseEvent.class),
	METRIC(8, MetricEvent.class),
	METRIC_META_DATA(9, MetricMetaDataEvent.class),
	METRIC_META_DATA_REQ(10, MetricMetaDataRequestEvent.class),
	
	STATE_INFO(11, StateUpdateInfoEvent.class)
	;
	private final int _id;
	private final Class<?> _clazz;
	
	private EventType(int id, Class<?> clazz) {
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
