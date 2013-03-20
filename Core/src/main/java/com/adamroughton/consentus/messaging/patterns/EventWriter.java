package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public interface EventWriter<TEvent extends ByteArrayBackedEvent> {

	/**
	 * Writes the content of an event using the given
	 * {@link ByteArrayBackedEvent} instance.
	 * @param event the event to write into
	 * @param sequence the sequence of the queue into which this entry will be
	 * written
	 * @return {@code true} if the write was successful, false if it should be
	 * discarded
	 */
	boolean write(TEvent event, long sequence);
	
}
