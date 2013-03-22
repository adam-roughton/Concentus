package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public interface EventReader<TEvent extends ByteArrayBackedEvent> {

	/**
	 * Reads the content of an event using the given
	 * {@link ByteArrayBackedEvent} instance.
	 * @param event the event to read from
	 */
	void read(TEvent event);
	
}
