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

import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SocketSetImpl implements SocketSet {

	private final Int2IntMap _socketLookup;
	private final ZmqSocketMessenger[] _socketMessengers;
	
	public SocketSetImpl(ZmqSocketMessenger... socketMessengers) {
		_socketLookup = new Int2IntArrayMap(socketMessengers.length);
		_socketLookup.defaultReturnValue(-1);
		_socketMessengers = socketMessengers;
		for (int i = 0; i < _socketMessengers.length; i++) {
			_socketLookup.put(socketMessengers[i].getSocketId(), i);
		}
	}
	
	public final ZmqSocketMessenger getMessenger(int socketId) {
		int socketIndex = _socketLookup.get(socketId);
		if (socketIndex != -1) {
			return _socketMessengers[socketIndex];
		} else {
			return null;
		}
	}
	
	public final boolean hasSocket(int socketId) {
		return _socketLookup.containsKey(socketId);
	}
	
	protected final ZmqSocketMessenger[] getSocketMessengers() {
		return _socketMessengers;
	}
	
	public Set<ZmqSocketMessenger> getMessengers() {
		return new HashSet<>(Arrays.asList(_socketMessengers));
	}
	
}