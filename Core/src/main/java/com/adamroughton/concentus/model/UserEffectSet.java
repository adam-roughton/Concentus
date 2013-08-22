package com.adamroughton.concentus.model;

import com.adamroughton.concentus.messaging.ResizingBuffer;

public interface UserEffectSet {

	ClientId getClientId();
	
	long getClientIdBits();
	
	boolean hasEffectFor(int variableId);
	
	boolean cancelEffect(int variableId);
	
	Effect getEffect(int variableId);
	
	boolean newEffect(int variableId, int effectTypeId, byte[] effectData);
	
	boolean newEffect(int variableId, int effectTypeId, byte[] effectData, int offset, int length);
	
	boolean newEffect(int variableId, int effectTypeId, ResizingBuffer effectData);
	
	boolean newEffect(int variableId, int effectTypeId, ResizingBuffer effectData, int offset, int length);
	
}
