package com.adamroughton.concentus.disruptor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public abstract class EventQueueBase<T> implements EventQueue<T> {
	
	private final RingBuffer<T> _ringBuffer;
	private final Set<Sequence> _gatingSequences;
	
	public EventQueueBase(RingBuffer<T> ringBuffer) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_gatingSequences = new HashSet<>();
	}

	@Override
	public final EventQueuePublisher<T> createPublisher(boolean isBlocking) {
		return doCreatePublisher(_ringBuffer, isBlocking);
	}
	
	protected abstract EventQueuePublisher<T> doCreatePublisher(RingBuffer<T> ringBuffer, boolean isBlocking);

	@Override
	public final EventQueueReader<T> createReader(boolean isBlocking, Sequence...gatingSequences) {
		SequenceBarrier barrier = _ringBuffer.newBarrier(gatingSequences);
		EventQueueReaderImpl<T> queueReader = new EventQueueReaderImpl<>(_ringBuffer, barrier, isBlocking);
		if (_gatingSequences.add(queueReader.getSequence())) {
			_ringBuffer.setGatingSequences(_gatingSequences.toArray(new Sequence[0]));
		}
		return queueReader;
	}

	@Override
	public final void setGatingSequences(Sequence... additionalSequences) {
		Collections.addAll(_gatingSequences, additionalSequences);
		_ringBuffer.setGatingSequences(_gatingSequences.toArray(new Sequence[0]));
		_gatingSequences.clear();
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
	public final BatchEventProcessor<T> createBatchEventProcessor(final EventHandler<T> eventHandler,
			Sequence... gatingSequences) {
		return createEventProcessor(new EventProcessorFactory<T, BatchEventProcessor<T>>() {

			@Override
			public BatchEventProcessor<T> createProcessor(
					RingBuffer<T> ringBuffer, SequenceBarrier barrier) {
				return new BatchEventProcessor<>(ringBuffer, barrier, eventHandler);
			}
		}, gatingSequences);
	}

	@Override
	public final DeadlineBasedEventProcessor<T> createDeadlineBasedEventProcessor(
			final DeadlineBasedEventHandler<T> eventHandler, final Clock clock,
			final FatalExceptionCallback exceptionCallback, Sequence... gatingSequences) {
		return createEventProcessor(new EventProcessorFactory<T, DeadlineBasedEventProcessor<T>>() {

			@Override
			public DeadlineBasedEventProcessor<T> createProcessor(RingBuffer<T> ringBuffer,
					SequenceBarrier barrier) {
				return new DeadlineBasedEventProcessor<>(clock, eventHandler, ringBuffer, barrier, exceptionCallback);
			}
		}, gatingSequences);
	}

	@Override
	public final <TProcessor extends EventProcessor> TProcessor createEventProcessor(EventProcessorFactory<T, TProcessor> processorFactory,
			Sequence... gatingSequences) {
		SequenceBarrier barrier = _ringBuffer.newBarrier(gatingSequences);
		TProcessor eventProcessor = processorFactory.createProcessor(_ringBuffer, barrier);
		if (_gatingSequences.add(eventProcessor.getSequence())) {
			_ringBuffer.setGatingSequences(_gatingSequences.toArray(new Sequence[0]));
		}
		return eventProcessor;
	}
	
}
