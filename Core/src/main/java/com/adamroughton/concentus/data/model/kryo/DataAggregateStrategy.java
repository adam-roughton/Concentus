package com.adamroughton.concentus.data.model.kryo;

public abstract class DataAggregateStrategy implements CandidateValueStrategy {
	
	private static final long serialVersionUID = 1L;

	@Override
	public final boolean canUnion(CandidateValue val1, CandidateValue val2) {
		return true;
	}

	@Override
	public final CandidateValue union(CandidateValue val1, CandidateValue val2) {
		return new CandidateValue(val1.getStrategy(), val1.getVariableId(), val1.getScore() + val2.getScore(), aggregate(val1, val2));
	}
	
	protected abstract byte[] aggregate(CandidateValue val1, CandidateValue val2);

	@Override
	public final CandidateValueGroupKey createGroupKey(CandidateValue value) {
		return new VariableIdKey(value);
	}
	
	public final class VariableIdKey implements CandidateValueGroupKey {

		private static final long serialVersionUID = 1L;
		
		private CandidateValue _value;
		private int _variableId;
		
		/*
		 * For Kryo
		 */
		@SuppressWarnings("unused")
		private VariableIdKey() {
		}
		
		public VariableIdKey(CandidateValue candidateValue) {
			_value = candidateValue;
			_variableId = _value.getVariableId();
		}
				
		public CandidateValue getValue() {
			return _value;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + _variableId;
			return result;
		}
		
		public int variableId() {
			return _variableId;
		}

		@Override
		public boolean equals(Object obj) {
			if (!(obj instanceof VariableIdKey))
				return false;
			VariableIdKey other = (VariableIdKey) obj;
			return this.variableId() == other.variableId();
		}
	}

}
