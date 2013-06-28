package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public interface EventQueueStrategy<T> {

	SequenceBarrier newBarrier(Sequence...sequencesToTrack);
	
	DataProvider<T> newQueueReader(String readerName);
	
	EventQueuePublisher<T> newQueuePublisher(String publisherName, boolean isBlocking);
	
	long getCursor();
	
	int getLength();
	
	long remainingCapacity();
	
	void addGatingSequences(Sequence... sequences);
	
	boolean removeGatingSequence(Sequence sequence);
	
	String getQueueName();
	
}
