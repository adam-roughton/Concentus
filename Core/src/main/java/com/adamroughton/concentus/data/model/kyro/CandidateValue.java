package com.adamroughton.concentus.data.model.kyro;

import java.util.Arrays;

import com.adamroughton.concentus.data.ResizingBuffer;

/**
 * Represents a possible value for a collective variable.
 * 
 * Naturally ordered by score, the hash of the value data, the variable ID, and then by time.
 * 
 * @author Adam Roughton
 *
 */
public final class CandidateValue implements Comparable<CandidateValue> {

	private int _variableId;
	private int _score;
	private int _valueDataHash;
	private long _time;
	
	//TODO defensive copy (valueDataHash depends on data remaining the same)
	private byte[] _valueData;
	
	/**
	 * For kyro 
	 */
	@SuppressWarnings("unused")
	private CandidateValue() {
	}
	
	public CandidateValue(int varId, int score, long time, ResizingBuffer data) {
		this(varId, score, time, data, 0, data.getContentSize());
	}
	
	public CandidateValue(int varId, int score, long time, ResizingBuffer data, int offset, int length) {
		this(varId, score, time, data.readBytes(offset, length));
	}
	
	public CandidateValue(int varId, int score, long time, byte[] data) {
		_variableId = varId;
		_score = score;
		_time = time;
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
	
	public long getTime() {
		return _time;
	}
	
	public byte[] getValueData() {
		return _valueData;
	}
	
	public static CandidateValue union(CandidateValue v1, CandidateValue v2) {
		if (v1.getVariableId() != v2.getVariableId()) {
			throw new IllegalArgumentException(String.format("The variable IDs must " +
					"match to perform a union: v1.id was %d, v2,id was %d",
					v1.getVariableId(),
					v2.getVariableId()));
		}
		if (v1.getTime() != v2.getTime()) {
			throw new IllegalArgumentException(String.format("The timestamps must " +
					"match to perform a union: v1.time was %d, v2.time was %d",
					v1.getTime(),
					v2.getTime()));
		}
		return new CandidateValue(v1.getVariableId(), v1.getScore() + v2.getScore(), v1.getTime(), v1.getValueData());
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
		else if (other._variableId != this._variableId)
			return other._variableId - this._variableId;
		else
			return (int) (other._time - this._time);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _score;
		result = prime * result + (int) (_time ^ (_time >>> 32));
		result = prime * result + Arrays.hashCode(_valueData);
		result = prime * result + _variableId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof CandidateValue))
			return false;
		CandidateValue other = (CandidateValue) obj;
		if (_score != other._score)
			return false;
		if (_time != other._time)
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
				+ _score + ", time=" + _time + ", valueData=" + Arrays.toString(_valueData) + "]";
	}
	
}
