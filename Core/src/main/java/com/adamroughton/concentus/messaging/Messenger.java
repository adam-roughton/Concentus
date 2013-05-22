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
package com.adamroughton.concentus.messaging;

public interface Messenger {

	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if the messenger is ready. If the header indicates the message is
	 * invalid, the message will be silently absorbed (i.e. the call will return
	 * {@code true}).
	 * @param outgoingBuffer the buffer to send from
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was sent OR the header indicated the message was invalid.
	 * @throws MessengerClosedException if the messenger has been closed
	 */
	boolean send(byte[] outgoingBuffer,
			OutgoingEventHeader header,
			boolean isBlocking) throws MessengerClosedException;
	
	/**
	 * Receives an event on the end points associated with this messenger, filling the given
	 * event buffer as per the given message parts policy.
	 * @param eventBuffer the buffer to place the event in
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was placed in the buffer
	 * @throws MessengerClosedException if the messenger has been closed
	 */
	 boolean recv(byte[] eventBuffer,
			IncomingEventHeader header,
			boolean isBlocking) throws MessengerClosedException;
	 
	 /**
	  * Gets all of the end point IDs associated with this messenger.
	  * @return an array of all end points associated with this messenger
	  */
	 int[] getEndpointIds();
	
}
