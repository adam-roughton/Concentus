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

public class HackSocketPollInSet extends SocketSetImpl implements SocketPollSet {

	private final ZmqSocketMessenger _socketMessenger;
	
	public HackSocketPollInSet(ZmqSocketMessenger... socketMessenger) {
		super(socketMessenger);
		if (socketMessenger.length != 1)
			throw new IllegalArgumentException("This hack only supports one socket");
		_socketMessenger = socketMessenger[0];
	}
	
	@Override
	public ZmqSocketMessenger poll() throws InterruptedException {
		return _socketMessenger;
	}
	
	@Override
	public ZmqSocketMessenger pollNoBlock() {
		return _socketMessenger;
	}
	
	@Override
	public ZmqSocketMessenger poll(long timeout, TimeUnit unit) throws InterruptedException {
		return _socketMessenger;
	}
	
}
