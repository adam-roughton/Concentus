package com.adamroughton.concentus.messaging.patterns;

import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;

public interface EventWriter<TSendHeader extends OutgoingEventHeader, TEvent extends ByteArrayBackedEvent> {

	/**
	 * Writes the content of an event using the given
	 * {@link ByteArrayBackedEvent} instance.
	 * @param header the header of the event
	 * @param event the event to write into
	 * @throws Exception if there is an error writing the event
	 */
	void write(TSendHeader header, TEvent event) throws Exception;
	
}
