package com.adamroughton.concentus.model;

public interface CandidateValue<T> {

	int getVariableId();
	
	T getValue();
	
	int getScore();
	
}
