/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.consentus.messaging;

import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.util.Arrays;
import java.util.Objects;

/**
 * Specifies the relationship between a buffer and
 * the ZMQ message parts that will be send/received over
 * the wire.
 * 
 * @author Adam Roughton
 *
 */
public final class MessageFrameBufferMapping {

	private final int[] _offsets;
	private final int _minReqBufferSize;
	private final Object2IntMap<String> _labelLookup;
	
	/**
	 * Creates a new policy from the given offsets. Offsets are processed in order,
	 * and each entry must be greater than or equal to the preceding entry.
	 * @param messagePartOffsets
	 * @throws IllegalArgumentException if any of the offsets are negative
	 */
	public MessageFrameBufferMapping(final int... messagePartOffsets) {
		int minBuffSize = 0;
		int lastOffset = 0;
		for (int offset : messagePartOffsets) {
			if (offset < 0) throw new IllegalArgumentException("All offsets must be greater than or equal to 0");
			if (offset < lastOffset) throw new IllegalArgumentException("Each offset must be greater than or equal to the preceding offset.");
			lastOffset = offset;
			minBuffSize = (offset + 1 > minBuffSize)? offset + 1 : minBuffSize;
		}
		_offsets = messagePartOffsets;
		_minReqBufferSize = minBuffSize;
		_labelLookup = new Object2IntArrayMap<>(messagePartOffsets.length);
	}
	
	public MessageFrameBufferMapping(final NamedOffset... messagePartOffsets) {
		this(getOffsets(messagePartOffsets));
		String[] labels = getLabels(messagePartOffsets);
		for (int i = 0; i < labels.length; i++) {
			_labelLookup.put(labels[i], i);
		}
	}
	
	/**
	 * Instantiates this policy with the properties of the given policy
	 * @param policyToClone
	 */
	public MessageFrameBufferMapping(final MessageFrameBufferMapping policyToClone) {
		Objects.requireNonNull(policyToClone);
		_offsets = Arrays.copyOf(policyToClone._offsets, policyToClone._offsets.length);
		_minReqBufferSize = policyToClone._minReqBufferSize;
		_labelLookup = new Object2IntArrayMap<>(policyToClone._labelLookup);
	}
	
	public void addLabel(final String label, final int partIndex) {
		if (partIndex < _offsets.length) {
		_labelLookup.put(label, partIndex);
		} else {
			throw new IllegalArgumentException(
					String.format("The part index was larger than the max: %d > %d", 
							partIndex, _offsets.length - 1));
		}
	}
	
	public void removeLabel(String label) {
		_labelLookup.remove(label);
	}
	
	public int getPartIndex(String label) {
		return _labelLookup.getInt(label);
	}
	
	/**
	 * Gets the offset of the given message part. No bounds checks
	 * are done, so it is up to the caller to ensure the given
	 * index is valid.
	 * @param partIndex the index of the message part
	 * @return the offset of the message part in the buffer
	 */
	public int getOffset(int partIndex) {
		return _offsets[partIndex];
	}
	
	/**
	 * The final message part is given the label
	 * {@literal content}. This method returns the
	 * offset of this message part. 
	 * @return the offset of the last message part
	 */
	public int getContentOffset() {
		if (_offsets.length > 0) {
			return _offsets[_offsets.length - 1];
		} else {
			return 0;
		}
	}
	
	public int getIntermediatePartLength(int partIndex)
		throws IllegalArgumentException {
		if (partIndex >= _offsets.length - 1)
			throw new IllegalArgumentException("The part index given does not map to an intermediate message part.");
		return _offsets[partIndex + 1] - _offsets[partIndex];
	}
	
	public int getContentLength(int bufferOffset, int bufferLength) {
		return bufferLength - bufferOffset - getContentOffset();
	}
	
	/**
	 * Gets the number of message parts this policy expects.
	 * @return the number of expected message parts
	 */
	public int partCount() {
		return _offsets.length;
	}
	
	/**
	 * Gets the minimum required buffer size for this policy.
	 * @return the minimum buffer size
	 */
	public int getMinReqBufferSize() {
		return _minReqBufferSize;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _minReqBufferSize;
		result = prime * result + Arrays.hashCode(_offsets);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageFrameBufferMapping other = (MessageFrameBufferMapping) obj;
		if (_minReqBufferSize != other._minReqBufferSize)
			return false;
		if (!Arrays.equals(_offsets, other._offsets))
			return false;
		return true;
	}
	
	private static int[] getOffsets(NamedOffset... namedOffsets) {
		int[] offsets = new int[namedOffsets.length];
		for (int i = 0; i < namedOffsets.length; i++) {
			offsets[i] = namedOffsets[i].getOffset();
		}
		return offsets;
	}
	
	private static String[] getLabels(NamedOffset... namedOffsets) {
		String[] labels = new String[namedOffsets.length];
		for (int i = 0; i < namedOffsets.length; i++) {
			labels[i] = namedOffsets[i].getLabel();
		}
		return labels;
	}
	
	public static class NamedOffset {
		private final int _offset;
		private final String _label;
		
		public NamedOffset(final int offset, final String label) {
			_offset = offset;
			_label = label;
		}
		
		public int getOffset() {
			return _offset;
		}
		
		public String getLabel() {
			return _label;
		}
	}
	
}
