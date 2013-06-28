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
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class EventQueueImpl<T> implements EventQueue<T> {

	protected final EventQueueStrategy<T> _queueStrategy;
	private final Set<Sequence> _gatingSequences = new HashSet<Sequence>();

	public EventQueueImpl(EventQueueStrategy<T> queueStrategy) {
		_queueStrategy = Objects.requireNonNull(queueStrategy);
	}

	@Override
	public final EventQueuePublisher<T> createPublisher(String publisherName, boolean isBlocking) {
		return _queueStrategy.newQueuePublisher(publisherName, isBlocking);
	}
	
	@Override
	public final EventQueueReader<T> createReader(String readerName, boolean isBlocking, Sequence...sequencesToTrack) {
		SequenceBarrier barrier = newBarrier(sequencesToTrack);
		EventQueueReaderImpl<T> queueReader = new EventQueueReaderImpl<>(readerName, _queueStrategy.newQueueReader(readerName), barrier, isBlocking);
		addGatingSequences(queueReader.getSequence());
		return queueReader;
	}

	@Override
	public final long getCursor() {
		return _queueStrategy.getCursor();
	}

	@Override
	public final long getQueueSize() {
		return _queueStrategy.getLength();
	}

	@Override
	public EventProcessor createEventProcessor(
			String processorName,
			final EventHandler<T> eventHandler,
			Sequence... sequencesToTrack) {
		return createEventProcessor(processorName, new EventProcessorFactory<T, BatchEventProcessor<T>>() {

			@Override
			public BatchEventProcessor<T> createProcessor(
					DataProvider<T> eventProvider, SequenceBarrier barrier) {
				return new BatchEventProcessor<>(eventProvider, barrier, eventHandler);
			}
		}, sequencesToTrack);
	}

	@Override
	public EventProcessor createEventProcessor(
			String processorName,
			final DeadlineBasedEventHandler<T> eventHandler, 
			final Clock clock,
			final FatalExceptionCallback exceptionCallback, 
			Sequence... sequencesToTrack) {
		return createEventProcessor(processorName, new EventProcessorFactory<T, DeadlineBasedEventProcessor<T>>() {

			@Override
			public DeadlineBasedEventProcessor<T> createProcessor(DataProvider<T> eventProvider,
					SequenceBarrier barrier) {
				return new DeadlineBasedEventProcessor<>(clock, eventHandler, eventProvider, barrier, exceptionCallback);
			}
		}, sequencesToTrack);
	}

	@Override
	public <TProcessor extends EventProcessor> TProcessor createEventProcessor(
			String processorName,
			EventProcessorFactory<T, TProcessor> processorFactory,
			Sequence... sequencesToTrack) {
		SequenceBarrier barrier = newBarrier(sequencesToTrack);
		TProcessor eventProcessor = processorFactory.createProcessor(_queueStrategy.newQueueReader(processorName), barrier);
		addGatingSequences(eventProcessor.getSequence());
		return eventProcessor;
	}

	@Override
	public void addGatingSequences(Sequence... sequences) {
		synchronized(_gatingSequences) {
			for (Sequence sequence : sequences) {
				if (_gatingSequences.add(sequence)) {
					_queueStrategy.addGatingSequences(sequence);
				}
			}
		}
	}
	
	@Override
	public boolean removeGatingSequence(Sequence sequence) {
		return _queueStrategy.removeGatingSequence(sequence);
	}

	@Override
	public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
		return _queueStrategy.newBarrier(sequencesToTrack);
	}

	@Override
	public String getName() {
		return _queueStrategy.getQueueName();
	}
	
}
