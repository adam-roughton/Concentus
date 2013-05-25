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

	void onEvent(T event, long sequence, boolean isEndOfBatch) throws Exception;

	/**
	 * Called when the next deadline is reached in a best effort manner.
	 */
	void onDeadline();
	
	/**
	 * Requests that the event handler move to the next deadline. If the previous
	 * deadline was skipped, {@code didSkipLast} will be {@code true}.
	 * @param unit the time unit in which to return the next deadline
	 * @param pendingCount the number of pending items in the buffer
	 * @return the next deadline in milliseconds since UTC
	 */
	long moveToNextDeadline(long pendingCount);
	
	/**
	 * Gets the next deadline without advancing.
	 * @return the next deadline in milliseconds since UTC
	 */
	long getDeadline();
	
}
