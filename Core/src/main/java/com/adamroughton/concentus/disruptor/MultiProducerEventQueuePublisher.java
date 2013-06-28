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

public final class MultiProducerEventQueuePublisher<T> implements EventQueuePublisher<T> {

	private final String _name;
	private final RingBuffer<T> _ringBuffer;
	private final EventEntryHandler<T> _entryHandler;
	private final boolean _isBlocking;
	private final T _tmpBuffer;
	private boolean _isPending;
	private long _lastPubSeq = -1;

	private PrePublishDelegate _prePublishDelegate = new NullPrePublishDelegate();
	
	public MultiProducerEventQueuePublisher(String publisherName, RingBuffer<T> ringBuffer, EventEntryHandler<T> entryHandler, 
			boolean isBlocking) {
		_name = Objects.requireNonNull(publisherName);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_entryHandler = Objects.requireNonNull(entryHandler);
		_tmpBuffer = entryHandler.newInstance();
		_isPending = false;
		_isBlocking = isBlocking;
	}

	@Override
	public T next() {
		if (!_isPending || publish()) {
			_isPending = true;
			return _tmpBuffer;
		} else {
			return null;
		}
	}

	@Override
	public boolean publish() {
		long seq;
		if (_isBlocking) {
			seq = _ringBuffer.next();
		} else {
			try {
				seq = _ringBuffer.tryNext();
			} catch (InsufficientCapacityException eNoCapacity) {
				return false;
			}
		}
		try {
			T queueBuffer = _ringBuffer.get(seq);
			_entryHandler.copy(_tmpBuffer, queueBuffer);
			_prePublishDelegate.beforePublish(seq);
		} finally {
			_ringBuffer.publish(seq);
		}
		
		_entryHandler.clear(_tmpBuffer);
		_isPending = false;
		
		return true;
	}

	@Override
	public boolean hasUnpublished() {
		return _isPending;
	}

	@Override
	public T getUnpublished() {
		return _tmpBuffer;
	}

	@Override
	public long getLastPublishedSequence() {
		return _lastPubSeq;
	}

	@Override
	public void setPrePublishDelegate(PrePublishDelegate delegate) {
		_prePublishDelegate = Objects.requireNonNull(delegate);
	}
	
	@Override
	public String getName() {
		return _name;
	}
	
}