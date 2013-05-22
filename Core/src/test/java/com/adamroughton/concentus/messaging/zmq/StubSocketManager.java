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

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.StubMessenger;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerFactory;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerMutex;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;

public class StubSocketManager implements SocketManager {

	private final StubMessengerConfigurator _configurator;
	private int _socketId = 0;
	private Int2ObjectMap<SocketSettings> _socketSettingsLookup = new Int2ObjectArrayMap<>();
	private Int2ObjectMap<MessengerMutex<StubMessenger>> _mutexLookup = new Int2ObjectArrayMap<>();
	private Int2ObjectMap<Int2ObjectMap<String>> _connectionStringLookup = new Int2ObjectArrayMap<>();
	
	public interface StubMessengerConfigurator {
		void onStubMessengerCreation(int socketId, StubMessenger messenger, int socketType, SocketSettings settings);
	}
	
	public StubSocketManager() {
		this(null);
	}
	
	public StubSocketManager(StubMessengerConfigurator configurator) {
		if (configurator == null) {
			configurator = new StubMessengerConfigurator() {
				
				@Override
				public void onStubMessengerCreation(int socketId, StubMessenger messenger,
						int socketType, SocketSettings settings) {
				}
			};
		}
		_configurator = configurator;
	}
	
	@Override
	public int create(int socketType) {
		return create(socketType, SocketSettings.create());
	}

	@Override
	public int create(int socketType, SocketSettings socketSettings) {
		int socketId = _socketId++;
		_socketSettingsLookup.put(socketId, socketSettings);
		StubMessenger stubMessenger = new StubMessenger(new int[] { socketId } );
		_configurator.onStubMessengerCreation(socketId, stubMessenger, socketType, socketSettings);
		_mutexLookup.put(socketId, new MessengerMutex<StubMessenger>(stubMessenger));
		return socketId;
	}

	@Override
	public SocketSettings getSettings(int socketId) {
		return _socketSettingsLookup.get(socketId);
	}

	@Override
	public void updateSettings(int socketId, SocketSettings socketSettings) {
		_socketSettingsLookup.put(socketId, socketSettings);
	}

	@Override
	public MessengerMutex<StubMessenger> getSocketMutex(int socketId) {
		return _mutexLookup.get(socketId);
	}

	@Override
	public MultiMessengerMutex<StubMessenger> createPollInSet(int... socketIds) {
		if (socketIds.length < 1) 
			throw new IllegalArgumentException("There must be at least one socket ID in the poll set.");
		ArrayList<MessengerMutex<StubMessenger>> messengerMutexes = new ArrayList<>(socketIds.length);
		for (int i = 0; i < socketIds.length; i++) {
			messengerMutexes.add(i, getSocketMutex(socketIds[i]));
		}
		return MessengerMutex.createMultiMessengerMutex(new MultiMessengerFactory<StubMessenger>() {

			@Override
			public StubMessenger create(Collection<StubMessenger> messengers) {
				int[] socketIds = new int[messengers.size()];
				int index = 0;
				for (StubMessenger messenger : messengers) {
					socketIds[index++] = messenger.getEndpointIds()[0];
				}
				return new StubMessenger(socketIds);
			}
			
		}, messengerMutexes);
	}

	@Override
	public int connectSocket(int socketId, String address) {
		Int2ObjectMap<String> socketConnMap;
		if (!_connectionStringLookup.containsKey(socketId)) {
			socketConnMap = new Int2ObjectArrayMap<String>();
			_connectionStringLookup.put(socketId, socketConnMap);
		} else {
			socketConnMap = _connectionStringLookup.get(socketId);
		}
		int connectionId = socketConnMap.size();
		socketConnMap.put(connectionId, address);
		return connectionId;
	}

	@Override
	public String disconnectSocket(int socketId, int connId) {
		return _connectionStringLookup.get(socketId).remove(connId);
	}

	@Override
	public void destroySocket(int socketId) {
		_socketSettingsLookup.remove(socketId);
		_mutexLookup.remove(socketId);
		_connectionStringLookup.remove(socketId);
	}

	@Override
	public void destroyAllSockets() {
		for (int socketId : new IntArraySet(_mutexLookup.keySet())) {
			destroySocket(socketId);
		}
	}
	
	@Override
	public void close() throws IOException {
		destroyAllSockets();
	}

}
