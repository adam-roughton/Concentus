package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.OutgoingEventHeader;

public interface SendTask<TSendHeader extends OutgoingEventHeader> {

	void write(final byte[] outgoingBuffer, final TSendHeader header);
	
}
