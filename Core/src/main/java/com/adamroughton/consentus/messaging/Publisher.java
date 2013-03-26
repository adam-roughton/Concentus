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
package com.adamroughton.consentus.messaging;

import java.util.Objects;
import org.zeromq.ZMQ;
import com.lmax.disruptor.EventHandler;

public class Publisher implements EventHandler<byte[]> {

	private final SocketPackage _socketPackage;
	private final EventSender _eventSender;
	
	public Publisher(final SocketPackage socketPackage,
			final OutgoingEventHeader header) {
		_socketPackage = Objects.requireNonNull(socketPackage);
		ZMQ.Socket socket = socketPackage.getSocket();
		if (socket.getType() != ZMQ.PUB && socket.getType() != ZMQ.XPUB) {
			throw new IllegalArgumentException(String.format("The socket type was %d, not PUB or XPUB", 
					socket.getType()));
		}
		_eventSender = new EventSender(header, false);
	}

	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		_eventSender.send(_socketPackage, event);
	}

}


