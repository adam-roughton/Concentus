package com.adamroughton.concentus.model;

public interface Effect {
	
	long getStartTime();
	
	int getVariableId();
	
	int getEffectTypeId();
	
	byte[] getData();
	
}
