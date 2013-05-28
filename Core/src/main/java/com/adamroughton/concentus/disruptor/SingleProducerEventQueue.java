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

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;

public class SingleProducerEventQueue<T> extends EventQueueBase<T> {

	public SingleProducerEventQueue(EventFactory<T> eventFactory, int size, WaitStrategy waitStrategy) {
		super(RingBuffer.createSingleProducer(eventFactory, size, waitStrategy));
	}
	
	@Override
	public boolean isShared() {
		return false;
	}

	@Override
	protected EventQueuePublisher<T> doCreatePublisher(
			RingBuffer<T> ringBuffer, boolean isBlocking) {
		return new SingleProducerEventQueuePublisher<>(ringBuffer, isBlocking);
	}

}
