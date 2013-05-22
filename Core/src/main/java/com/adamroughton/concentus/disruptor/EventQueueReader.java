package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public interface EventQueueReader<T> {

	T get() throws AlertException, InterruptedException;
	
	void advance();
	
	Sequence getSequence();
	
	SequenceBarrier getBarrier();
	
}
