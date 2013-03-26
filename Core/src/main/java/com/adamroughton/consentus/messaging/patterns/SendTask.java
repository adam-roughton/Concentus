package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.OutgoingEventHeader;

public interface SendTask {

	void write(final byte[] outgoingBuffer, final OutgoingEventHeader header);
	
}
