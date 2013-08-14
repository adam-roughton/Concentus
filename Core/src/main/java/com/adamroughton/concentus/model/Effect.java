package com.adamroughton.concentus.model;

public interface Effect<TData> {
	
	long getStartTime();
	
	int getVariableId();
	
	TData getData();
	
	int getEffectTypeId();
	
}
