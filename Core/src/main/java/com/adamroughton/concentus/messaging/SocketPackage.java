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

import org.zeromq.ZMQ;

public final class SocketPackage {
	
	private final ZMQ.Socket _socket;
	private final int _socketId;
	
	public static SocketPackage create(ZMQ.Socket socket) {
		return new SocketPackage(socket, 0);
	}
	
	private SocketPackage(
			final ZMQ.Socket socket,
			final int socketId) {
		_socket = socket;
		_socketId = socketId;
	}
	
	/**
	 * An optional identifier that will be written in to an attached
	 * buffer as part of a header.
	 * 
	 * @param socketId
	 * @return
	 */
	public SocketPackage setSocketId(final int socketId) {
		return new SocketPackage(_socket, socketId);
	}
	
	/**
	 * An optional identifier that will be written in to an attached
	 * buffer as part of a header.
	 * 
	 * @return the socket ID
	 */
	public int getSocketId() {
		return _socketId;
	}
	
	public ZMQ.Socket getSocket() {
		return _socket;
	}
}
