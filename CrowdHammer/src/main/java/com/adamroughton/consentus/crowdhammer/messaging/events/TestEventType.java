package com.adamroughton.consentus.crowdhammer.messaging.events;

import com.adamroughton.consentus.messaging.events.EventType;

public enum TestEventType {
	
	// INPUT
	STATE_INPUT(EventType.STATE_INPUT.getId(), EventType.STATE_INPUT.getEventClass()),
	STATE_UPDATE(EventType.STATE_UPDATE.getId(), EventType.STATE_UPDATE.getEventClass()),
	STATE_METRIC(EventType.STATE_METRIC.getId(), EventType.STATE_METRIC.getEventClass()),
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
