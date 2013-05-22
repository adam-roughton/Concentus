package com.adamroughton.concentus.messaging;

public interface EventQueuePublisher<T> {
	
	/**
	 * Attempts to get an entry that can be used for writing.
	 * @return an entry that can be used for writing the event, 
	 * or {@code null} if no free entry is available
	 */
	T next();
	
	/**
	 * Attempts to publish any pending entry. This operation does
	 * nothing if there is currently no pending entries.
	 * @return {@code true} if any pending buffer was successfully
	 * published OR no pending buffer was present; {@code false} if
	 * the pending buffer could not be published
	 */
	boolean publish();
	
	boolean hasUnpublished();
	
}
