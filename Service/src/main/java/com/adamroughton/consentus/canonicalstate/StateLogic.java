package com.adamroughton.consentus.canonicalstate;

import java.nio.ByteBuffer;

public interface StateLogic {

	void collectInput(ByteBuffer inputBuffer);
	
	void tick(long simTime, long timeDelta);
	
	void createUpdate(ByteBuffer updateBuffer);
}
