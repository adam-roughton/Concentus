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

public class SocketPollInSet extends SocketSetImpl implements SocketPollSet {

	private final ZMQ.Poller _poller;
	
	public SocketPollInSet(ZMQ.Context context, ZmqSocketMessenger... socketMessengers) {
		super(socketMessengers);
		_poller = new ZMQ.Poller(socketMessengers.length);
		for (int i = 0; i < socketMessengers.length; i++) {
			_poller.register(socketMessengers[i].getSocket(), ZMQ.Poller.POLLIN);
		}
	}
	
	public ZmqSocketMessenger poll() throws InterruptedException {
		while (!Thread.interrupted()) {
			long availableCount = _poller.poll();
			if (availableCount > 0) {
				return getReadySocket();
			}
		}
		throw new InterruptedException();
	}
	
	public ZmqSocketMessenger pollNoBlock() {
		int availableCount = _poller.poll(0);
		if (availableCount > 0) {
			return getReadySocket();
		} else {
			return null;
		}
	}
	
	public ZmqSocketMessenger poll(long timeout, TimeUnit unit) throws InterruptedException {
		long timeoutTime = System.nanoTime() + unit.toNanos(timeout);
		while (!Thread.interrupted() && System.nanoTime() < timeoutTime) {
			int availableCount = _poller.poll(unit.toMillis(timeout));
			if (availableCount > 0) {
				return getReadySocket();
			}
		}
		return null;
	}
	
	private ZmqSocketMessenger getReadySocket() {
		ZmqSocketMessenger[] socketMessengers = getSocketMessengers();
		for (int i = 0; i < socketMessengers.length; i++) {
			if (_poller.pollin(i)) return socketMessengers[i];
		}
		return null;
	}
	
}
