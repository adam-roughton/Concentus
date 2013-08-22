package com.adamroughton.concentus.model;

import java.util.Arrays;

import com.adamroughton.concentus.messaging.ResizingBuffer;

/**
 * Represents a possible value for a collective variable.
 * 
 * Naturally ordered by score, the hash of the value data, and then by the variable ID.
 * 
 * @author Adam Roughton
 *
 */
public class CandidateValue implements Comparable<CandidateValue> {

	private final int _variableId;
	private final int _score;
	private final int _valueDataHash;
	
	//TODO defensive copy (valueDataHash depends on data remaining the same)
	private final byte[] _valueData;
	
	public CandidateValue(int varId, int score, ResizingBuffer data) {
		this(varId, score, data, 0, data.getContentSize());
	}
	
	public CandidateValue(int varId, int score, ResizingBuffer data, int offset, int length) {
		this(varId, score, data.readBytes(offset, length));
	}
	
	public CandidateValue(int varId, int score, byte[] data) {
		_variableId = varId;
		_score = score;
		if (data == null) data = new byte[0];
		_valueData = data;
		_valueDataHash = Arrays.hashCode(_valueData);
	}
	
	public int getVariableId() {
		return _variableId;
	}
	
	public int getScore() {
		return _score;
	}
	
	public byte[] getValueData() {
		return _valueData;
	}
	
	public static CandidateValue union(CandidateValue v1, CandidateValue v2) {
		if (v1.getVariableId() != v2.getVariableId()) {
			throw new IllegalArgumentException(String.format("The variable IDs must " +
					"match to perform a union: v1Id was %d, v2Id was %d",
					v1.getVariableId(),
					v1.getVariableId()));
		}
		return new CandidateValue(v1.getVariableId(), v1.getScore() + v2.getScore(), v1.getValueData());
	}
	
	public CandidateValue union(CandidateValue other) {
		return union(this, other);
	}
	
	public boolean matchesValue(CandidateValue other) {
		if (other._valueDataHash == _valueDataHash) {
			return Arrays.equals(other._valueData, _valueData);
		} else {
			return false;
		}
	}
	
	@Override
	public int compareTo(CandidateValue other) {
		if (other._score != this._score)
			return other._score - this._score;
		else if (other._valueDataHash != this._valueDataHash)
			return other._valueDataHash - this._valueDataHash;
		else
			return other._variableId - this._variableId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _score;
		result = prime * result + _valueDataHash;
		result = prime * result + _variableId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof CandidateValue))
			return false;
		CandidateValue other = (CandidateValue) obj;
		if (_score != other._score)
			return false;
		if (!Arrays.equals(_valueData, other._valueData))
			return false;
		if (_variableId != other._variableId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CandidateValue [variableId=" + _variableId + ", score="
				+ _score + ", valueData=" + Arrays.toString(_valueData) + "]";
	}
	
}
