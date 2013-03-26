package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public interface EventWriter<TEvent extends ByteArrayBackedEvent> {

	/**
	 * Writes the content of an event using the given
	 * {@link ByteArrayBackedEvent} instance.
	 * @param event the event to write into
	 * @throws Exception if there is an error writing the event
	 */
	void write(TEvent event) throws Exception;
	
}
