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
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public interface EventQueue<T> {
	
	EventQueuePublisher<T> createPublisher(String publisherName, boolean isBlocking);
	
	/**
	 * Creates a new queue reader that waits on the given sequences and the cursor
	 * of the underlying ring buffer.
	 * @param isBlocking
	 * @return
	 */
	EventQueueReader<T> createReader(String readerName, boolean isBlocking, Sequence...sequencesToTrack);
	
	/**
	 * Creates an event processor that wraps the provided event handler.
	 * @param eventHandler the event handler to wrap
	 * @return an event processor that consumes events on the queue
	 */
	EventProcessor createEventProcessor(String processorName, EventHandler<T> eventHandler, Sequence... sequencesToTrack);
	
	EventProcessor createEventProcessor(String processorName, DeadlineBasedEventHandler<T> eventHandler, Clock clock, 
			FatalExceptionCallback exceptionCallback, Sequence... sequencesToTrack);
	
	<TProcessor extends EventProcessor> TProcessor createEventProcessor(String processorName, EventProcessorFactory<T, TProcessor> processorFactory, Sequence...sequencesToTrack);
	
	void addGatingSequences(Sequence...sequences);
	
	boolean removeGatingSequence(Sequence sequence);
	
	SequenceBarrier newBarrier(Sequence...sequencesToTrack);
	
	/**
	 * @see {@link RingBuffer#getCursor()}
	 */
	long getCursor();
	
	/**
	 * @see {@link RingBuffer#getBufferSize()}
	 * @return
	 */
	long getQueueSize();
	
	String getName();
	
	/**
	 * Factory for creating event processors that consume events from
	 * an event queue.
	 * @author Adam Roughton
	 *
	 * @param <TEvent> the event type
	 * @param <TProcessor> the processor type
	 */
	public interface EventProcessorFactory<TEvent, TProcessor extends EventProcessor> {
		TProcessor createProcessor(DataProvider<TEvent> eventProvider, SequenceBarrier barrier);
	}
}
