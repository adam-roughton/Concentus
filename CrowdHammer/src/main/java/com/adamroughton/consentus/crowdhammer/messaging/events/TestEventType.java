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
package com.adamroughton.consentus.crowdhammer.messaging.events;

import com.adamroughton.consentus.messaging.events.EventType;

public enum TestEventType {
	CLIENT_INPUT(EventType.CLIENT_INPUT.getId(), EventType.CLIENT_INPUT.getEventClass()),
	STATE_INPUT(EventType.STATE_INPUT.getId(), EventType.STATE_INPUT.getEventClass()),
	STATE_UPDATE(EventType.STATE_UPDATE.getId(), EventType.STATE_UPDATE.getEventClass()),
	STATE_METRIC(EventType.STATE_METRIC.getId(), EventType.STATE_METRIC.getEventClass()),
	STATE_INFO(EventType.STATE_INFO.getId(), EventType.STATE_INFO.getEventClass()),
	LOAD_METRIC(100, LoadMetricEvent.class)
	;
	private final int _id;
	private final Class<?> _clazz;
	
	private TestEventType(final int id, final Class<?> clazz) {
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