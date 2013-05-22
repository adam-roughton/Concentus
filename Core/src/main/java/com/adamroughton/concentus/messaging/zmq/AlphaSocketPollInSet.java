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

import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

public class AlphaSocketPollInSet extends SocketSetImpl implements SocketPollSet {

	public AlphaSocketPollInSet(ZmqSocketMessenger... socketMessengers) {
		super(socketMessengers);
	}
	
	@Override
	public ZmqSocketMessenger poll() throws InterruptedException {
		while (!Thread.interrupted()) {
			ZmqSocketMessenger readyMessenger = pollNoBlock();
			if (readyMessenger != null)
				return readyMessenger;
		}
		throw new InterruptedException();
	}
	
	@Override
	public ZmqSocketMessenger pollNoBlock() {
		for (ZmqSocketMessenger socketMessenger : getSocketMessengers()) {
			if ((socketMessenger.getSocket().getEvents() & ZMQ.Poller.POLLIN) != 0) {
				return socketMessenger;
			}
		}
		return null;
	}
	
	@Override
	public ZmqSocketMessenger poll(long timeout, TimeUnit unit) throws InterruptedException {
		long timeoutTime = System.nanoTime() + unit.toNanos(timeout);
		while (!Thread.interrupted() && System.nanoTime() < timeoutTime) {
			ZmqSocketMessenger readyMessenger = pollNoBlock();
			if (readyMessenger != null)
				return readyMessenger;
		}
		return null;
	}
	
}
