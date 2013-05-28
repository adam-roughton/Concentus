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

public class SingleProducerEventQueuePublisher<T> implements EventQueuePublisher<T> {

	private final RingBuffer<T> _ringBuffer;
	private final boolean _isBlocking;
	
	/**
	 * Track the sequence of the unpublished entry sequence, or {@code -1}
	 * if no entry has been claimed.
	 */
	private long _unpubClaimedSeq = -1;
	
	public SingleProducerEventQueuePublisher(final RingBuffer<T> ringBuffer, boolean isBlocking) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_isBlocking = isBlocking;
	}
	
	/**
	 * Attempts to claim an entry that can be used for writing. If an
	 * entry has already been allocated previously, but not published,
	 * this call will <b>not</b> allocate another entry.
	 * @return the entry that has been claimed, or {@code null} if an entry could
	 * not be claimed
	 */
	public final T next() {
		if (_unpubClaimedSeq == -1) {
			if (_isBlocking) {
				_unpubClaimedSeq = _ringBuffer.next();
			} else {
				try {
					_unpubClaimedSeq = _ringBuffer.tryNext();			
				} catch (InsufficientCapacityException eNoCapacity) {
					_unpubClaimedSeq = -1;
				}
			}
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
	public boolean publish() {
		if (hasUnpublished()) {
			_ringBuffer.publish(_unpubClaimedSeq);
			_unpubClaimedSeq = -1;
		}
		return true;
	}
	
	public boolean hasUnpublished() {
		return _unpubClaimedSeq != -1;
	}

	@Override
	public T getUnpublished() {
		if (hasUnpublished()) {
			return _ringBuffer.get(_unpubClaimedSeq);
		} else {
			return null;
		}
	}
	
}
