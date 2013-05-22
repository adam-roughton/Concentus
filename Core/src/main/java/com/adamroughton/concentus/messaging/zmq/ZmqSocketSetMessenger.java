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
package com.adamroughton.concentus.messaging.zmq;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public class ZmqSocketSetMessenger implements Messenger {

	private final SocketPollSet _pollSet;
	private final int[] _socketIds;
	
	public ZmqSocketSetMessenger(SocketPollSet pollSet) {
		_pollSet = Objects.requireNonNull(pollSet);
		Set<ZmqSocketMessenger> messengers = pollSet.getMessengers();
		_socketIds = new int[messengers.size()];
		int index = 0;
		for (ZmqSocketMessenger messenger : messengers) {
			_socketIds[index++] = messenger.getSocketId();
		}
	}
	
	@Override
	public boolean send(byte[] outgoingBuffer, OutgoingEventHeader header,
			boolean isBlocking) {
		if (!header.isValid(outgoingBuffer)) return true;
		
		int socketId = header.getTargetSocketId(outgoingBuffer);
		ZmqSocketMessenger messenger = _pollSet.getMessenger(socketId);
		if (messenger == null) {
			throw new RuntimeException(String.format("The messenger associated with ID %d was not found in the messenger set.", socketId));
		}
		return messenger.send(outgoingBuffer, header, isBlocking);
	}

	@Override
	public boolean recv(byte[] eventBuffer, IncomingEventHeader header,
			boolean isBlocking) {
		ZmqSocketMessenger readyMessenger;
		if (isBlocking) {
			try {
				readyMessenger = _pollSet.poll();
			} catch (InterruptedException eInterrupted) {
				Thread.currentThread().interrupt();
				return false;
			}
		} else {
			readyMessenger = _pollSet.pollNoBlock();
		}
		if (readyMessenger != null) {
			return readyMessenger.recv(eventBuffer, header, isBlocking);
		} else {
			return false;
		}
	}

	@Override
	public int[] getEndpointIds() {
		return Arrays.copyOf(_socketIds, _socketIds.length);
	}

	@Override
	public boolean hasPendingEvents() {
		return _pollSet.pollNoBlock() != null;
	}

}
