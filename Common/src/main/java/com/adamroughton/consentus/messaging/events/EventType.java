package com.adamroughton.consentus.messaging.events;

public enum EventType {
	// INPUT
	STATE_INPUT(2, StateInputEvent.class),
	STATE_UPDATE(3, StateUpdateEvent.class),
	STATE_METRIC(4, StateMetricEvent.class)
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
