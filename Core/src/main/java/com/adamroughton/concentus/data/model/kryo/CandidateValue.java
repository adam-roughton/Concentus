package com.adamroughton.concentus.data.model.kryo;

import java.util.Arrays;
import java.util.Objects;

import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ResizingBuffer;

import static com.adamroughton.concentus.data.ResizingBuffer.*;

/**
 * Represents a possible value for a collective variable.
 * 
 * Naturally ordered by score, the length of the value data, the variable ID, and then by data content.
 * 
 * @author Adam Roughton
 *
 */
public final class CandidateValue implements Comparable<CandidateValue> {

	private CandidateValueGroupKey _groupKey;
	private CandidateValueStrategy _strategy;
	private int _variableId;
	private int _score;
	private int _valueDataHash;
	
	//TODO defensive copy (valueDataHash depends on data remaining the same)
	private byte[] _valueData;
	
	/**
	 * For kyro 
	 */
	@SuppressWarnings("unused")
	private CandidateValue() {
	}
	
	public CandidateValue(CandidateValueStrategy strategy, int varId, int score, ResizingBuffer data) {
		this(strategy, varId, score, data, 0, data.getContentSize());
	}
	
	public CandidateValue(CandidateValueStrategy strategy, int varId, int score, ResizingBuffer data, int offset, int length) {
		this(strategy, varId, score, data.readBytes(offset, length));
	}
	
	public CandidateValue(CandidateValueStrategy strategy, int varId, int score, byte[] data) {
		_strategy = Objects.requireNonNull(strategy);
		_groupKey = _strategy.createGroupKey(this);
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
	
	public int getValueDataHash() {
		return _valueDataHash;
	}
	
	public CandidateValueStrategy getStrategy() {
		return _strategy;
	}
	
	public boolean canUnion(CandidateValue other) {
		return this.getVariableId() == other.getVariableId() && _strategy.canUnion(this, other);
	}
	
	public CandidateValue union(CandidateValue other) {
		return _strategy.union(this, other);
	}
	
	@Override
	public int compareTo(CandidateValue other) {
		if (other._score != this._score)
			return other._score - this._score;
		else if (other._valueData.length != this._valueData.length)
			return other._valueData.length - this._valueData.length;
		else if (other._variableId != this._variableId)
			return other._variableId - this._variableId;
		else {
			// compare the data segments in long, int and then byte chunks
			int[] chunkSegmentLengths = new int[] { LONG_SIZE, INT_SIZE, 1 };
			
			byte[] thisData = this._valueData;
			byte[] otherData = other._valueData;
			
			int cursor = 0;
			for (int chunkLength : chunkSegmentLengths) {
				int chunkCount = (thisData.length - cursor) / chunkLength;
				for (int i = 0; i < chunkCount; i++) {
					long otherDataSeg = readChunk(otherData, cursor + i * chunkLength, chunkLength);
					long thisDataSeg = readChunk(thisData, cursor + i * chunkLength, chunkLength);
					if (otherDataSeg != thisDataSeg) {
						return (int) (otherDataSeg - thisDataSeg);
					} 
				}
				cursor += chunkCount * chunkLength;
			}
			return 0;
		}
	}
	
	private long readChunk(byte[] data, int offset, int chunkLength) {
		switch (chunkLength) {
		case 8:
			return BytesUtil.readLong(data, offset);
		case 4:
			return BytesUtil.readInt(data, offset);
		case 1:
			return data[offset];
		default:
			throw new IllegalArgumentException("Cannot read data in chunks of " + chunkLength);
		}
	}
	
	public CandidateValueGroupKey groupKey() {
		return _groupKey;
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
		if (!(obj instanceof CandidateValue))
			return false;
		CandidateValue other = (CandidateValue) obj;
		if (_score != other._score)
			return false;
		if (_variableId != other._variableId)
			return false;
		if (!Arrays.equals(_valueData, other._valueData))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CandidateValue [variableId=" + _variableId + ", score="
				+ _score + ", valueData=" + Arrays.toString(_valueData) + "]";
	}
	

	
	
}
