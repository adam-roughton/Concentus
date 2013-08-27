package com.adamroughton.concentus.data.model;


public interface Effect {
	
	long getStartTime();
	
	long getClientIdBits();
	
	ClientId getClientId();
	
	int getVariableId();
	
	int getEffectTypeId();
	
	boolean isCancelled();
	
	byte[] getData();
	
}
