package com.adamroughton.consentus.disruptor;

import java.util.Objects;

import com.lmax.disruptor.RingBuffer;

public class NonBlockingRingBufferWriter<T> {

	private final RingBuffer<T> _ringBuffer;
	
	/**
	 * Track the sequence of the unpublished entry sequence, or {@code -1}
	 * if no entry has been claimed.
	 */
	private long _unpubClaimedSeq = -1;
	
	public NonBlockingRingBufferWriter(final RingBuffer<T> ringBuffer) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
	}
	
	/**
	 * Attempts to claim an entry that can be used for writing. If an
	 * entry has already been allocated previously, but not published,
	 * this call will <b>not</b> allocate another entry.
	 * @return the entry that has been claimed, or {@code null} if an entry could
	 * not be claimed
	 */
	public final T claimNoBlock() {
		if (_unpubClaimedSeq == -1 && _ringBuffer.hasAvailableCapacity(1)) {
			_unpubClaimedSeq = _ringBuffer.next();					
		}	
		if (_unpubClaimedSeq != -1) {
			return _ringBuffer.get(_unpubClaimedSeq);
		} else {
			return null;
		}
	}
	
	/**
	 * Publishes any pending claimed entry. This operation does
	 * nothing if there is currently no claimed entry.
	 */
	public final void publish() {
		if (_unpubClaimedSeq != -1) {
			_ringBuffer.publish(_unpubClaimedSeq);
			_unpubClaimedSeq = -1;
		}
	}
	
	/**
	 * Alias for publish.
	 */
	public void tidyUp() {
		publish();
	}

}
