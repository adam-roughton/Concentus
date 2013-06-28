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
package com.adamroughton.concentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public class SendQueue<TSendHeader extends OutgoingEventHeader> {

	private final TSendHeader _header;
	private final EventQueuePublisher<byte[]> _sendQueuePublisher;
	
	public SendQueue(
			String name,
			TSendHeader header, 
			EventQueue<byte[]> sendQueue) {
		_header = Objects.requireNonNull(header);
		_sendQueuePublisher = sendQueue.createPublisher(name, true);
	}
	
	/**
	 * Flag indicating whether the queue was full at the time
	 * of the call. This indicates whether the queue will block
	 * on the next send (assuming it remains full).
	 * @return {@code true} if the queue is full
	 */
	public final boolean isFull() {
		return _sendQueuePublisher.hasUnpublished();
	}
	
	public final void send(SendTask<TSendHeader> task) {
		byte[] outgoingBuffer = _sendQueuePublisher.next();
		try {
			_header.reset(outgoingBuffer);
			task.write(outgoingBuffer, _header);
		} finally {
			_sendQueuePublisher.publish();
		}
	}
	
	//TODO: this is wrong - need to semantically ensure that
	// an entry is checked and claimed atomically to avoid
	// blocking on multi-producer ring buffers
	/**
	 * Sends the task if there is space, failing if the call
	 * would block. This call is not thread safe.
	 * @param task the task to send
	 * @return {@code true} if the task was send without blocking,
	 * {@code false} otherwise
	 */
	public final boolean trySend(SendTask<TSendHeader> task) {
		boolean canSendNoBlock = !isFull();
		if (canSendNoBlock) {
			send(task);
		}
		return canSendNoBlock;
	}
	
}
