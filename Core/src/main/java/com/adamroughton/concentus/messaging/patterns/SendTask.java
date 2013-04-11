package com.adamroughton.concentus.messaging.patterns;

import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public interface SendTask<TSendHeader extends OutgoingEventHeader> {

	void write(final byte[] outgoingBuffer, final TSendHeader header);
	
}
