package com.adamroughton.concentus.model;

public interface Effect {
	
	long getStartTime();
	
	long getClientIdBits();
	
	ClientId getClientId();
	
	int getVariableId();
	
	int getEffectTypeId();
	
	boolean isCancelled();
	
	byte[] getData();
	
}
