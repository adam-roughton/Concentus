package com.adamroughton.concentus.messaging.patterns;

import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;

public interface EventReader<TRecvHeader extends IncomingEventHeader, TEvent extends ByteArrayBackedEvent> {

	/**
	 * Reads the content of an event using the given
	 * {@link ByteArrayBackedEvent} instance.
	 * @param header the header of the event
	 * @param event the event to read from
	 */
	void read(TRecvHeader header, TEvent event);
	
}
