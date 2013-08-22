package com.adamroughton.concentus.model;

public interface CollectiveVariableSet {

	CollectiveVariable getVariable(int variableId);
	
	boolean hasVariable(int variableId);
	
	// collective variable set
	
}
