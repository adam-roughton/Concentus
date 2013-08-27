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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.messaging.StubMessenger;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerFactory;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerMutex;
import com.adamroughton.concentus.messaging.StubMessenger.FakeRecvDelegate;
import com.adamroughton.concentus.messaging.StubMessenger.FakeSendDelegate;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;

public class StubSocketManager<TBuffer extends ResizingBuffer> implements SocketManager<TBuffer> {

	private final StubMessengerConfigurator<TBuffer> _configurator;
	private final BufferFactory<TBuffer> _bufferFactory;
	private int _socketId = 0;
	private Int2ObjectMap<SocketSettings> _socketSettingsLookup = new Int2ObjectArrayMap<>();
	private Int2ObjectMap<MessengerMutex<TBuffer, StubMessenger<TBuffer>>> _mutexLookup = new Int2ObjectArrayMap<>();
	private Int2ObjectMap<Int2ObjectMap<String>> _connectionStringLookup = new Int2ObjectArrayMap<>();
	
	public interface StubMessengerConfigurator<TBuffer extends ResizingBuffer> {
		void onStubMessengerCreation(int socketId, StubMessenger<TBuffer> messenger, int socketType, SocketSettings settings);
	}
	
	public StubSocketManager(BufferFactory<TBuffer> bufferFactory) {
		this(bufferFactory, null);
	}
	
	public StubSocketManager(BufferFactory<TBuffer> bufferFactory, StubMessengerConfigurator<TBuffer> configurator) {
		if (configurator == null) {
			configurator = new StubMessengerConfigurator<TBuffer>() {
				
				@Override
				public void onStubMessengerCreation(int socketId, StubMessenger<TBuffer> messenger,
						int socketType, SocketSettings settings) {
				}
			};
		}
		_configurator = configurator;
		_bufferFactory = Objects.requireNonNull(bufferFactory);
	}
	
	@Override
	public BufferFactory<TBuffer> getBufferFactory() {
		return _bufferFactory;
	}
	
	@Override
	public MessageQueueFactory<TBuffer> newMessageQueueFactory(
			EventQueueFactory eventQueueFactory) {
		return new MessageQueueFactory<>(eventQueueFactory, _bufferFactory);
	}
	
	@Override
	public int create(int socketType, String name) {
		return create(socketType, SocketSettings.create(), name);
	}

	@Override
	public int create(int socketType, SocketSettings socketSettings, String name) {
		int socketId = _socketId++;
		_socketSettingsLookup.put(socketId, socketSettings);
		StubMessenger<TBuffer> stubMessenger = new StubMessenger<TBuffer>(name, new int[] { socketId } );
		_configurator.onStubMessengerCreation(socketId, stubMessenger, socketType, socketSettings);
		_mutexLookup.put(socketId, new MessengerMutex<TBuffer, StubMessenger<TBuffer>>(stubMessenger));
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
	public MessengerMutex<TBuffer, StubMessenger<TBuffer>> getSocketMutex(int socketId) {
		return _mutexLookup.get(socketId);
	}

	@Override
	public MultiMessengerMutex<TBuffer, Messenger<TBuffer>, StubMessenger<TBuffer>> createPollInSet(int... socketIds) {
		if (socketIds.length < 1) 
			throw new IllegalArgumentException("There must be at least one socket ID in the poll set.");
		ArrayList<MessengerMutex<TBuffer, StubMessenger<TBuffer>>> messengerMutexes = new ArrayList<>(socketIds.length);
		for (int i = 0; i < socketIds.length; i++) {
			messengerMutexes.add(i, getSocketMutex(socketIds[i]));
		}
		return MessengerMutex.createMultiMessengerMutex(new MultiMessengerFactory<TBuffer, Messenger<TBuffer>, StubMessenger<TBuffer>>() {

			@Override
			public StubMessenger<TBuffer> create(Collection<StubMessenger<TBuffer>> messengers) {
				final ArrayList<StubMessenger<TBuffer>> messengersList = new ArrayList<>(messengers);
				final Int2ObjectMap<StubMessenger<TBuffer>> messengerLookup = new Int2ObjectArrayMap<>();
				int[] socketIds = new int[messengers.size()];
				int index = 0;
				StringBuilder builder = new StringBuilder();
				builder.append("set[");
				boolean isFirst = true;
				for (StubMessenger<TBuffer> messenger : messengers) {
					socketIds[index++] = messenger.getEndpointIds()[0];
					messengerLookup.put(messenger.getEndpointIds()[0], messenger);
					builder.append(messenger.name());						
					if (isFirst) {
						isFirst = false;
					} else{
						builder.append(", ");
					}
				}
				builder.append("]");
				StubMessenger<TBuffer> multiMessenger = new StubMessenger<>(builder.toString(), socketIds);
				multiMessenger.setFakeRecvDelegate(new FakeRecvDelegate<TBuffer>() {
					
					@Override
					public boolean fakeRecv(int[] endPointIds, long recvSeq,
							TBuffer eventBuffer, IncomingEventHeader header, boolean isBlocking) {
						return messengersList.get((int)(recvSeq % messengersList.size())).recv(eventBuffer, header, isBlocking);
					}
				});
				multiMessenger.setFakeSendDelegate(new FakeSendDelegate<TBuffer>() {
					
					@Override
					public boolean fakeSend(long sendSeq, TBuffer eventBuffer,
							OutgoingEventHeader header, boolean isBlocking) {
						int socketId = header.getTargetSocketId(eventBuffer);
						return messengerLookup.get(socketId).send(eventBuffer, header, isBlocking);
					}
				});
				return multiMessenger;
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

	@Override
	public SocketIdentity resolveIdentity(int socketId,
			String connectionString, long timeout, TimeUnit timeUnit)
			throws InterruptedException, TimeoutException,
			UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int[] getBoundPorts(int socketId) {
		return new int[0];
	}

}
