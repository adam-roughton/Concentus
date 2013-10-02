package com.adamroughton.concentus.data.model.kryo;

public interface CandidateValueStrategy {

	boolean canUnion(CandidateValue val1, CandidateValue val2);
	
	CandidateValue union(CandidateValue val1, CandidateValue val2);
	
	CandidateValueGroupKey createGroupKey(CandidateValue value);
	
}
