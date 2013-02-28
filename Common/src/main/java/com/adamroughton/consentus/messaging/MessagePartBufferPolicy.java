package com.adamroughton.consentus.messaging;

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
public final class MessagePartBufferPolicy {

	private final int[] _offsets;
	private final int _minReqBufferSize;
	
	/**
	 * Creates a new policy from the given offsets. Offsets are processed in order,
	 * and each entry must be greater than or equal to the preceding entry.
	 * @param messagePartOffsets
	 * @throws IllegalArgumentException if any of the offsets are negative
	 */
	public MessagePartBufferPolicy(final int... messagePartOffsets) {
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
	}
	
	/**
	 * Instantiates this policy with the properties of the given policy
	 * @param policyToClone
	 */
	public MessagePartBufferPolicy(final MessagePartBufferPolicy policyToClone) {
		Objects.requireNonNull(policyToClone);
		_offsets = Arrays.copyOf(policyToClone._offsets, policyToClone._offsets.length);
		_minReqBufferSize = policyToClone._minReqBufferSize;
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
		MessagePartBufferPolicy other = (MessagePartBufferPolicy) obj;
		if (_minReqBufferSize != other._minReqBufferSize)
			return false;
		if (!Arrays.equals(_offsets, other._offsets))
			return false;
		return true;
	}
	
}
