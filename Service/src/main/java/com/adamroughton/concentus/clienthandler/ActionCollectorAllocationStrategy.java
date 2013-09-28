package com.adamroughton.concentus.clienthandler;

import org.javatuples.Pair;

import com.adamroughton.concentus.messaging.SocketIdentity;

interface ActionCollectorAllocationStrategy {
	
	Pair<Integer, SocketIdentity> allocateClient(long clientId);
	
}
