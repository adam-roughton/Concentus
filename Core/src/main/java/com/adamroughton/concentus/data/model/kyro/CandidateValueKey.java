package com.adamroughton.concentus.data.model.kyro;

import java.util.Arrays;
import java.util.Objects;

public final class CandidateValueKey {

	private int _cachedHash;
	private CandidateValue _candidateValue;
	
	/*
	 * For Kryo
	 */
	private CandidateValueKey() {
	}
	
	public CandidateValueKey(CandidateValue value) {
		_candidateValue = Objects.requireNonNull(value);
		
		
		
	}
	
	public CandidateValue getValue() {
		return _candidateValue;
	}

	@Override
	public int hashCode() {
		return _cachedHash;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof CandidateValueKey))
			return false;
		CandidateValueKey other = (CandidateValueKey) obj;
		if (this._cachedHash != other._cachedHash)
			return false;
		if (this._candidateValue.getVariableId() != other._candidateValue.getVariableId())
			return false;
		return Arrays.equals(this._candidateValue.getValueData(), other._candidateValue.getValueData());
	}
}
