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

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;

public final class SharedEventQueue<T> extends EventQueueBase<T> {
	
	private final EventEntryHandler<T> _entryHandler;
	
	public SharedEventQueue(EventEntryHandler<T> entryHandler, ClaimStrategy claimStrategy, WaitStrategy waitStrategy) {
		super(new RingBuffer<>(entryHandler, claimStrategy, waitStrategy));
		_entryHandler = Objects.requireNonNull(entryHandler);
	}

	@Override
	public boolean isShared() {
		return true;
	}

	@Override
	protected EventQueuePublisher<T> doCreatePublisher(
			RingBuffer<T> ringBuffer, boolean isBlocking) {
		return new MultiProducerEventQueuePublisher<>(ringBuffer, _entryHandler, isBlocking);
	}
	
}
