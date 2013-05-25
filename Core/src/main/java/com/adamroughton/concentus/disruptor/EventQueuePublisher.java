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
	
	T getUnpublished();
	
}
