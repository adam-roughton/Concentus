package com.adamroughton.concentus.data.model.kyro;

import java.util.Arrays;

public final class CollectiveVariable {

	private int _variableId;
	private CandidateValue[] _candidateValues;
	private int _valueCount;
	private long _time;
	
	/**
	 * For kyro
	 */
	@SuppressWarnings("unused")
	private CollectiveVariable() {
	}
	
	public CollectiveVariable(int topCount, int variableId, long time) {
		this(variableId, time, new CandidateValue[topCount], 0);
	}
	
	private CollectiveVariable(int variableId, long time, CandidateValue[] candidateValues, int valueCount) {
		_variableId = variableId;
		_time = time;
		_candidateValues = candidateValues;
		_valueCount = valueCount;
	}
	
	public boolean push(CandidateValue value) {
		if (value.getVariableId() == _variableId && value.getTime() == _time) {
			if (_valueCount < _candidateValues.length) {
				_candidateValues[_valueCount++] = value;
				Arrays.sort(_candidateValues, 0, _valueCount);
				return true;
			} else {
				int lastIndex = _candidateValues.length - 1;
				CandidateValue lastValue = _candidateValues[lastIndex];
				if (lastValue.compareTo(value) > 0) {
					_candidateValues[lastIndex] = value;
					Arrays.sort(_candidateValues, 0, _valueCount);
					return true;
				} else {
					return false;
				}
			}
		} else {
			return false;
		}
	}
	
	public static CollectiveVariable union(CollectiveVariable var1, CollectiveVariable var2) {
		if (var1.getVariableId() != var2.getVariableId()) {
			throw new IllegalArgumentException(String.format("The variable IDs must " +
					"match to perform a union: var1Id was %d, var2Id was %d",
					var1.getVariableId(),
					var2.getVariableId()));
		}
		if (var1.getTime() != var2.getTime()) {
			throw new IllegalArgumentException(String.format("The timestamps must " +
					"match to perform a union: var1.time was %d, var2.time was %d",
					var1.getTime(),
					var2.getTime()));
		}
		int variableId = var1.getVariableId();
		CandidateValue[] var1Values = var1._candidateValues;
		CandidateValue[] var2Values = var2._candidateValues;
		CandidateValue[] mergedValues = new CandidateValue[var1Values.length];
		
		int var1Index = 0;
		int var2Index = 0;
		int var1Count = var1.getValueCount();
		int var2Count = var2.getValueCount();
		int mergeIndex = 0;
		for (; mergeIndex < mergedValues.length; mergeIndex++) {
			CandidateValue nextValue;
			if (var1Index < var1Count && var2Index < var2Count) {
				if (var1Values[var1Index].compareTo(var2Values[var2Index]) > 0) {
					nextValue = var2Values[var2Index++];
				} else {
					nextValue = var1Values[var1Index++];
				}
			} else if (var1Index < var1Count) {
				nextValue = var1Values[var1Index++];
			} else if (var2Index < var2Count) {
				nextValue = var2Values[var2Index++];
			} else {
				mergeIndex++;
				break;
			}
			mergedValues[mergeIndex] = nextValue;		
		}
		int mergedValueCount = mergeIndex;
		return new CollectiveVariable(variableId, var1.getTime(), mergedValues, mergedValueCount);
	}
	
	public CollectiveVariable union(CollectiveVariable other) {
		return union(this, other);
	}
	
	public int getValueCount() {
		return _valueCount;
	}
	
	public long getTime() {
		return _time;
	}
	
	public CandidateValue getValue(int index) {
		return _candidateValues[index];
	}
	
	public int getVariableId() {
		return _variableId;
	}
}
