package com.adamroughton.concentus.clienthandler;

import com.adamroughton.concentus.messaging.SocketIdentity;

interface ActionProcessorAllocationStrategy {
	
	SocketIdentity allocateClient(long clientId);
	
}
