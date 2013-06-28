package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public abstract class EventQueueStrategyBase<T> implements EventQueueStrategy<T> {

	protected final RingBuffer<T> _ringBuffer;
	protected String _queueName;
	
	public EventQueueStrategyBase(String queueName, RingBuffer<T> ringBuffer) {
		_queueName = Objects.requireNonNull(queueName);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
	}

	@Override
	public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
		return _ringBuffer.newBarrier(sequencesToTrack);
	}

	@Override
	public DataProvider<T> newQueueReader(String readerName) {
		return _ringBuffer;
	}

	@Override
	public long getCursor() {
		return _ringBuffer.getCursor();
	}

	@Override
	public int getLength() {
		return _ringBuffer.getBufferSize();
	}

	@Override
	public long remainingCapacity() {
		return _ringBuffer.remainingCapacity();
	}

	@Override
	public void addGatingSequences(Sequence... sequences) {
		_ringBuffer.addGatingSequences(sequences);
	}

	@Override
	public boolean removeGatingSequence(Sequence sequence) {
		return _ringBuffer.removeGatingSequence(sequence);
	}

	@Override
	public String getQueueName() {
		return _queueName;
	}
	
}
