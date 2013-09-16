package com.adamroughton.concentus.data.model.kryo;

import java.util.Arrays;

public final class CollectiveVariable {

	private int _variableId;
	private CandidateValue[] _candidateValues;
	private int _valueCount;
	
	/**
	 * For kyro
	 */
	@SuppressWarnings("unused")
	private CollectiveVariable() {
	}
	
	public CollectiveVariable(int topCount, int variableId) {
		this(variableId, new CandidateValue[topCount], 0);
	}
	
	public CollectiveVariable(int topCount, CandidateValue initialValue) {
		this(initialValue.getVariableId(), initWithElement(initialValue, topCount), 1);
	}
	
	private static CandidateValue[] initWithElement(CandidateValue initValue, int count) {
		if (count <= 0) throw new IllegalArgumentException("The count must be at least one to accomodate the initial value.");
		CandidateValue[] values = new CandidateValue[count];
		values[0] = initValue;
		return values;
	}
	
	private CollectiveVariable(int variableId, CandidateValue[] candidateValues, int valueCount) {
		_variableId = variableId;
		_candidateValues = candidateValues;
		_valueCount = valueCount;
	}
	
	public boolean push(CandidateValue value) {
		if (value.getVariableId() == _variableId) {
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
		return new CollectiveVariable(variableId, mergedValues, mergedValueCount);
	}
	
	public CollectiveVariable union(CollectiveVariable other) {
		return union(this, other);
	}
	
	public int getValueCount() {
		return _valueCount;
	}
	
	public CandidateValue getValue(int index) {
		return _candidateValues[index];
	}
	
	public int getVariableId() {
		return _variableId;
	}
}
