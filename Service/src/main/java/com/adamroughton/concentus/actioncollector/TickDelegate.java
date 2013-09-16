package com.adamroughton.concentus.actioncollector;

import java.util.Iterator;

import com.adamroughton.concentus.data.model.kryo.CandidateValue;

public interface TickDelegate {
	void onTick(long time, Iterator<CandidateValue> candidateValues);
}
