package com.adamroughton.concentus.model;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.model.Effect;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
import com.adamroughton.concentus.data.model.kryo.CollectiveVariable;

public interface CollectiveApplication {

	/* 
	 * TODO access control to variables (e.g. some crowd members have access to some but not others)
	 * I imagine this would relate to the client topology, so this will probably need to be reasoned
	 * with first (i.e. once allocated to a position in the topology, allocate access to collective
	 * variables.
	 * Topology manager (centralised place where clients are allocated and client numbers are tracked)
	 */
	
	void processAction(UserEffectSet effectSet, int actionTypeId, ResizingBuffer actionData);
	
	/**
	 * The application should apply the relevant effect function to the effect using the given
	 * time to generate a candidate value for the target collective variable. If this effect has expired,
	 * the returned candidate value should have a negative score.
	 * @param effect
	 * @param time
	 * @return
	 */
	CandidateValue apply(Effect effect, long time);
	
	long getTickDuration();
	
	CollectiveVariableDefinition[] variableDefinitions();
	
	void createUpdate(ResizingBuffer updateData, long time, Int2ObjectMap<CollectiveVariable> variables);
	
}
