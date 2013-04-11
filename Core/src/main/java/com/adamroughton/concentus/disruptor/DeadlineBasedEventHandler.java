package com.adamroughton.concentus.disruptor;

import java.util.concurrent.TimeUnit;

/**
 * Processes events with {@link DeadlineBasedEventHandler#onEvent(Object, long, long)} 
 * until the next deadline upon which {@link DeadlineBasedEventHandler#nextDeadline(TimeUnit)} is called.
 * This is performed in a best effort way, with no guarantees that the deadline will be called on time.
 * 
 * @author Adam Roughton
 *
 * @param <T>
 */
public interface DeadlineBasedEventHandler<T> {

	void onEvent(T event, long sequence, long nextDeadline) throws Exception;

	/**
	 * Called when the next deadline is reached in a best effort manner.
	 */
	void onDeadline();
	
	/**
	 * Requests that the event handler move to the next deadline. If the previous
	 * deadline was skipped, {@code didSkipLast} will be {@code true}.
	 * @param unit the time unit in which to return the next deadline
	 * @param forcedEventCount the number of events that were processed ahead of the
	 * deadline so as to clear the backlog
	 * @return the next deadline in milliseconds since UTC
	 */
	long moveToNextDeadline(long forcedEventCount);
	
	/**
	 * Gets the next deadline without advancing.
	 * @return the next deadline in milliseconds since UTC
	 */
	long getDeadline();
	
}
