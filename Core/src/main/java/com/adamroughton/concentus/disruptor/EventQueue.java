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

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public interface EventQueue<T> {
	
	/**
	 * Flag indicating whether this event queue is shared between two or
	 * more event producers. This is useful for determining whether entries
	 * can be claimed and held during blocking calls (entries from shared queues 
	 * should never be held while blocking). Exclusive ownership of the queue
	 * can be utilised for blocking zero copy operations.
	 * @return {@code true} if this queue is shared between two or more event
	 * producers, {@code false} otherwise
	 */
	boolean isShared();
	
	EventQueuePublisher<T> createPublisher(boolean isBlocking);
	
	/**
	 * Creates a new queue reader that waits on the given sequences and the cursor
	 * of the underlying ring buffer.
	 * @param isBlocking
	 * @return
	 */
	EventQueueReader<T> createReader(boolean isBlocking, Sequence...gatingSequences);
	
	/**
	 * Creates an event processor that wraps the provided event handler.
	 * @param eventHandler the event handler to wrap
	 * @return an event processor that consumes events on the queue
	 */
	BatchEventProcessor<T> createBatchEventProcessor(EventHandler<T> eventHandler, Sequence... gatingSequences);
	
	DeadlineBasedEventProcessor<T> createDeadlineBasedEventProcessor(DeadlineBasedEventHandler<T> eventHandler, Clock clock, 
			FatalExceptionCallback exceptionCallback, Sequence... gatingSequences);
	
	<TProcessor extends EventProcessor> TProcessor createEventProcessor(EventProcessorFactory<T, TProcessor> processorFactory, Sequence...gatingSequences);
	
	/**
	 * Finalises the set of sequences that the underlying ring buffer
	 * gates on, including both the sequences given and those of created
	 * event queue readers.
	 * @param additionalSequences the sequences to gate on in addition to those
	 * of created event queue readers.
	 */
	void setGatingSequences(Sequence...additionalSequences);
	
	/**
	 * @see {@link RingBuffer#getCursor()}
	 */
	long getCursor();
	
	/**
	 * @see {@link RingBuffer#getBufferSize()}
	 * @return
	 */
	long getQueueSize();
	
	/**
	 * Factory for creating event processors that consume events from
	 * an event queue.
	 * @author Adam Roughton
	 *
	 * @param <TEvent> the event type
	 * @param <TProcessor> the processor type
	 */
	public interface EventProcessorFactory<TEvent, TProcessor extends EventProcessor> {
		TProcessor createProcessor(RingBuffer<TEvent> ringBuffer, SequenceBarrier barrier);
	}
}
