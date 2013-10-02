package com.adamroughton.concentus.data.model;

import com.adamroughton.concentus.data.model.kryo.CandidateValue;

public interface CollectiveVariable {

	boolean push(CandidateValue value);
	
	void union(CollectiveVariable other);
	
}
