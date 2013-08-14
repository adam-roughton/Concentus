package com.adamroughton.concentus.model;

public interface EffectFunction<TValue, TData> {

	CandidateValue<TValue> apply(Effect<TData> effect, long time);
	
}
