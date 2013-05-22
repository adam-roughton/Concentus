package com.adamroughton.concentus.messaging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public abstract class EventQueueBase<T> implements EventQueue<T> {
	
	private final RingBuffer<T> _ringBuffer;
	private final ArrayList<Sequence> _gatingSequences;
	private boolean _hasGated;
	
	public EventQueueBase(RingBuffer<T> ringBuffer) {
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_gatingSequences = new ArrayList<>();
		_hasGated = false;
	}

	@Override
	public final EventQueuePublisher<T> createPublisher(boolean isBlocking) {
		if (_hasGated) throw new IllegalStateException("The event queue has already been set up.");
		return doCreatePublisher(_ringBuffer, isBlocking);
	}
	
	protected abstract EventQueuePublisher<T> doCreatePublisher(RingBuffer<T> ringBuffer, boolean isBlocking);

	@Override
	public final EventQueueReader<T> createReader(boolean isBlocking, Sequence...sequencesToGateOn) {
		if (_hasGated) throw new IllegalStateException("The event queue has already been set up.");
		SequenceBarrier barrier = _ringBuffer.newBarrier(sequencesToGateOn);
		EventQueueReaderImpl<T> queueReader = new EventQueueReaderImpl<>(_ringBuffer, barrier, isBlocking);
		_gatingSequences.add(queueReader.getSequence());
		return queueReader;
	}

	@Override
	public final void setGatingSequences(Sequence... additionalSequences) {
		if (_hasGated) throw new IllegalStateException("The event queue has already been set up.");
		Collections.addAll(_gatingSequences, additionalSequences);
		_ringBuffer.setGatingSequences(_gatingSequences.toArray(new Sequence[0]));
		_gatingSequences.clear();
		_hasGated = true;
	}
	
}
