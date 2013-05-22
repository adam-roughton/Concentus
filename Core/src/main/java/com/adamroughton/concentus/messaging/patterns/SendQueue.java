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

import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.lmax.disruptor.RingBuffer;

public class SendQueue<TSendHeader extends OutgoingEventHeader> {

	private final TSendHeader _header;
	private final RingBuffer<byte[]> _ringBuffer;
	
	public SendQueue(final TSendHeader header, 
			final RingBuffer<byte[]> buffer) {
		_header = Objects.requireNonNull(header);
		_ringBuffer = Objects.requireNonNull(buffer);
	}
	
	public final void send(SendTask<TSendHeader> task) {
		long seq = _ringBuffer.next();
		try {
			byte[] outgoingBuffer = _ringBuffer.get(seq);
			_header.reset(outgoingBuffer);
			task.write(outgoingBuffer, _header);
		} finally {
			_ringBuffer.publish(seq);
		}
	}
	
}
