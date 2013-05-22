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

import java.io.Closeable;

import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.util.Mutex;

public interface SocketManager extends Closeable {
	
	/**
	 * Creates a new managed socket. The socket is not opened until
	 * a call to {@link SocketManager#updateSettings(int, SocketSettings)} is made
	 * with a {@link SocketSettings} object that includes a port to bind to; or 
	 * a call to {@link SocketManager#connectSocket(int, String)} is made.
	 * @param socketType the ZMQ socket type
	 * @return the socketID which refers to the created socket
	 */
	int create(final int socketType);
	
	/**
	 * Creates a new managed socket. The socket is opened immediately if
	 * the {@link SocketSettings} object includes a port to bind to; otherwise
	 * the socket will remain closed until a call to 
	 * {@link SocketManager#connectSocket(int, String)} is made, or the settings
	 * are updated with a bound port through {@link SocketManager#updateSettings(int, SocketSettings)}.
	 * @param socketType the ZMQ socket type
	 * @return the socketID which refers to the created socket
	 */
	int create(final int socketType, SocketSettings socketSettings);
	
	/**
	 * Get the settings associated with the given socket ID.
	 * @param socketId the socket ID
	 * @return the socket settings
	 */
	SocketSettings getSettings(final int socketId);
	
	/**
	 * Updates the settings associated with the given socket. If the socket
	 * is already open, the socket will first be closed. 
	 * If the settings include a port to bind to, or their are
	 * connections associated with the socket, the socket will be opened
	 * before this call returns.
	 * @param socketId the ID of the socket to update
	 * @param socketSettings the new settings for the socket
	 * @throws IllegalStateException if the socket is in use by another thread
	 */
	void updateSettings(final int socketId, final SocketSettings socketSettings);
	
	Mutex<Messenger> getSocketMutex(final int socketId);
	
	Mutex<Messenger> createPollInSet(int... socketIds);
	
	int connectSocket(final int socketId, final String address);
	
	String disconnectSocket(final int socketId, final int connId);
	
	void destroySocket(int socketId);
	
	void destroyAllSockets();
	
}


