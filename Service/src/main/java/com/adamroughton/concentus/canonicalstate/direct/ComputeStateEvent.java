package com.adamroughton.concentus.canonicalstate.direct;

import java.util.ArrayList;

import com.adamroughton.concentus.data.model.kryo.CandidateValue;

final class ComputeStateEvent {

	public long time;
	public final ArrayList<CandidateValue> candidateValues = new ArrayList<>();
	
}
