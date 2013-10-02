package com.adamroughton.concentus.data.model.kryo;

import java.util.Arrays;

public final class MatchingDataStrategy implements CandidateValueStrategy {
	
	@Override
	public boolean canUnion(CandidateValue val1, CandidateValue val2) {
		return val1.getValueDataHash() == val2.getValueDataHash() && Arrays.equals(val1.getValueData(), val2.getValueData());
	}

	@Override
	public CandidateValue union(CandidateValue val1, CandidateValue val2) {
		return new CandidateValue(val1.getStrategy(), val1.getVariableId(), val1.getScore() + val2.getScore(), val1.getValueData());
	}

	@Override
	public CandidateValueGroupKey createGroupKey(CandidateValue value) {
		return new MatchingDataKey(value);
	}
	
	public final class MatchingDataKey implements CandidateValueGroupKey {

		private CandidateValue _value;
		private int _variableId;
		private byte[] _valueData;
		private int _valueDataHash;
		
		/*
		 * For Kryo
		 */
		@SuppressWarnings("unused")
		private MatchingDataKey() {
		}
		
		public MatchingDataKey(CandidateValue candidateValue) {
			_value = candidateValue;
			_variableId = _value.getVariableId();
			_valueData = _value.getValueData();
			_valueDataHash = _value.getValueDataHash();
		}
				
		public CandidateValue getValue() {
			return _value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + _valueDataHash;
			result = prime * result + _variableId;
			return result;
		}
		
		public int variableId() {
			return _variableId;
		}
		
		public int valueDataHash() {
			return _valueDataHash;
		}
		
		public byte[] valueData() {
			return _valueData;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof MatchingDataKey))
				return false;
			MatchingDataKey other = (MatchingDataKey) obj;
			if (this.variableId() != other.variableId())
				return false;
			if (this.valueDataHash() != other.valueDataHash())
				return false;
			return Arrays.equals(this.valueData(), other.valueData());
		}
	}

}
