package com.adamroughton.consentus.messaging.events;

public final class ClientHandlerEntry {

	private final int _clientHandlerId;
	private final long _highestHandlerSeq;
	
	public ClientHandlerEntry(int clientHandlerId, long highestClientHandlerSeq) {
		_clientHandlerId = clientHandlerId;
		_highestHandlerSeq = highestClientHandlerSeq;
	}
	
	public int getClientHandlerId() {
		return _clientHandlerId;
	}

	public long getHighestHandlerSeq() {
		return _highestHandlerSeq;
	}
	
}
