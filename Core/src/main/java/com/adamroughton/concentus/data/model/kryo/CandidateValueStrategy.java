package com.adamroughton.concentus.data.model.kryo;

import java.io.Serializable;

public interface CandidateValueStrategy extends Serializable {

	boolean canUnion(CandidateValue val1, CandidateValue val2);
	
	CandidateValue union(CandidateValue val1, CandidateValue val2);
	
	CandidateValueGroupKey createGroupKey(CandidateValue value);
	
}
