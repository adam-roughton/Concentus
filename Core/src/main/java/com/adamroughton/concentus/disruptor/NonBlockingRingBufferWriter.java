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
package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.lmax.disruptor.InsufficientCapacityException;
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
		if (_unpubClaimedSeq == -1) {
			try {
				_unpubClaimedSeq = _ringBuffer.tryNext(1);			
			} catch (InsufficientCapacityException eNoCapacity) {
				_unpubClaimedSeq = -1;
			}
		}	
		if (_unpubClaimedSeq != -1) {
			return _ringBuffer.get(_unpubClaimedSeq);
		} else {
			return null;
		}
	}
	
	public long getUnpublishedSeq() {
		return _unpubClaimedSeq;
	}
	
	/**
	 * Publishes any pending claimed entry. This operation does
	 * nothing if there is currently no claimed entry.
	 */
	public final void publish() {
		if (hasUnpublished()) {
			_ringBuffer.publish(_unpubClaimedSeq);
			_unpubClaimedSeq = -1;
		}
	}
	
	public final boolean hasUnpublished() {
		return _unpubClaimedSeq != -1;
	}
	
	public final T getUnpublished() {
		if (hasUnpublished()) {
			return _ringBuffer.get(_unpubClaimedSeq);
		} else {
			return null;
		}
	}

}
