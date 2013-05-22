package com.adamroughton.concentus.messaging;

import com.lmax.disruptor.Sequence;

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
	 * Finalises the set of sequences that the underlying ring buffer
	 * gates on, including both the sequences given and those of created
	 * event queue readers.
	 * @param additionalSequences the sequences to gate on in addition to those
	 * of created event queue readers.
	 */
	void setGatingSequences(Sequence...additionalSequences);
}
