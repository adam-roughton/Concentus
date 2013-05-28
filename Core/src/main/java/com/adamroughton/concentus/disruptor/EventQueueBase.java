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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public abstract class EventQueueBase<T> implements EventQueue<T> {

	protected final RingBuffer<T> _ringBuffer;
	private final Set<Sequence> _gatingSequences = new HashSet<Sequence>();
	
	public EventQueueBase(RingBuffer<T> ringBuffer) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
	}

	@Override
	public final EventQueuePublisher<T> createPublisher(boolean isBlocking) {
		return doCreatePublisher(_ringBuffer, isBlocking);
	}
	
	protected abstract EventQueuePublisher<T> doCreatePublisher(RingBuffer<T> ringBuffer, boolean isBlocking);

	@Override
	public final EventQueueReader<T> createReader(boolean isBlocking, Sequence...sequencesToTrack) {
		SequenceBarrier barrier = newBarrier(sequencesToTrack);
		EventQueueReaderImpl<T> queueReader = new EventQueueReaderImpl<>(_ringBuffer, barrier, isBlocking);
		addGatingSequences(queueReader.getSequence());
		return queueReader;
	}

	@Override
	public final long getCursor() {
		return _ringBuffer.getCursor();
	}

	@Override
	public final long getQueueSize() {
		return _ringBuffer.getBufferSize();
	}

	@Override
	public EventProcessor createEventProcessor(final EventHandler<T> eventHandler,
			Sequence... sequencesToTrack) {
		return createEventProcessor(new EventProcessorFactory<T, BatchEventProcessor<T>>() {

			@Override
			public BatchEventProcessor<T> createProcessor(
					DataProvider<T> eventProvider, SequenceBarrier barrier) {
				return new BatchEventProcessor<>(eventProvider, barrier, eventHandler);
			}
		}, sequencesToTrack);
	}

	@Override
	public EventProcessor createEventProcessor(
			final DeadlineBasedEventHandler<T> eventHandler, final Clock clock,
			final FatalExceptionCallback exceptionCallback, Sequence... sequencesToTrack) {
		return createEventProcessor(new EventProcessorFactory<T, DeadlineBasedEventProcessor<T>>() {

			@Override
			public DeadlineBasedEventProcessor<T> createProcessor(DataProvider<T> eventProvider,
					SequenceBarrier barrier) {
				return new DeadlineBasedEventProcessor<>(clock, eventHandler, eventProvider, barrier, exceptionCallback);
			}
		}, sequencesToTrack);
	}

	@Override
	public <TProcessor extends EventProcessor> TProcessor createEventProcessor(EventProcessorFactory<T, TProcessor> processorFactory,
			Sequence... sequencesToTrack) {
		SequenceBarrier barrier = newBarrier(sequencesToTrack);
		TProcessor eventProcessor = processorFactory.createProcessor(_ringBuffer, barrier);
		addGatingSequences(eventProcessor.getSequence());
		return eventProcessor;
	}

	@Override
	public void addGatingSequences(Sequence... sequences) {
		synchronized(_gatingSequences) {
			for (Sequence sequence : sequences) {
				if (_gatingSequences.add(sequence)) {
					_ringBuffer.addGatingSequences(sequence);
				}
			}
		}
	}
	
	@Override
	public boolean removeGatingSequence(Sequence sequence) {
		return _ringBuffer.removeGatingSequence(sequence);
	}

	@Override
	public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
		return _ringBuffer.newBarrier(sequencesToTrack);
	}
	
}
