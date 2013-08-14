package com.adamroughton.concentus.model;

public interface Action {

	int getVariableId();
	
	int getActionTypeId();
	
	byte[] getData();
	
}
