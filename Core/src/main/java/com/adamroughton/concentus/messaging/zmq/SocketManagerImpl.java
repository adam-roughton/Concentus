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
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerFactory;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerMutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;

public final class SocketManagerImpl implements SocketManager {

	private static final int SOCKET_TIMEOUT = 1000;
	
	private final Clock _clock;
	private final ZMQ.Context _zmqContext;
	private final Int2IntMap _socketTypeLookup = new Int2IntArrayMap();
	private final Int2ObjectMap<ConnectionsRecord> _socketConnLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<MessengerMutex<ZmqSocketMessenger>> _socketMutexLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<SocketSettings> _settingsLookup = new Int2ObjectArrayMap<>();
	private int _nextSocketId = 0;
	private boolean _isActive = true;
	
	public SocketManagerImpl(Clock clock) {
		this(clock, 1);
	}
	
	public SocketManagerImpl(Clock clock, int ioThreads) {
		_clock = Objects.requireNonNull(clock);
		_zmqContext = ZMQ.context(ioThreads);
	}
	
	/**
	 * Creates a new managed socket. The socket is not opened until
	 * a call to {@link SocketManager#updateSettings(int, SocketSettings)} is made
	 * with a {@link SocketSettings} object that includes a port to bind to; or 
	 * a call to {@link SocketManager#connectSocket(int, String)} is made.
	 * @param socketType the ZMQ socket type
	 * @return the socketID which refers to the created socket
	 */
	public synchronized int create(int socketType) {
		return create(socketType, SocketSettings.create());
	}
	
	public synchronized int create(int socketType, SocketSettings socketSettings) {
		assertManagerActive();
		int socketId = _nextSocketId++;
		_socketTypeLookup.put(socketId, socketType);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
		_socketConnLookup.put(socketId, new ConnectionsRecord());
		newSocket(socketId);
		return socketId;
	}
	
	private void newSocket(int socketId) {
		int socketType = _socketTypeLookup.get(socketId);
		SocketSettings socketSettings = _settingsLookup.get(socketId);
		ZMQ.Socket socket = _zmqContext.socket(socketType);
		socket.setReceiveTimeOut(SOCKET_TIMEOUT);
		socket.setSendTimeOut(SOCKET_TIMEOUT);
		socket.setLinger(0);
		ZmqSocketMessenger messenger = new ZmqSocketMessenger(socketId, socket, _clock);
		_socketMutexLookup.put(socketId, new MessengerMutex<>(messenger));
		configureSocket(socket, socketSettings);
		for (String connectionString : _socketConnLookup.get(socketId).getAllConnectionStrings()) {
			socket.connect(connectionString);
		}
	}
	
	private void configureSocket(ZMQ.Socket socket, SocketSettings settings) {
		long hwm = settings.getHWM();
		if (hwm != -1) {
			socket.setHWM(hwm);
		}
		for (byte[] subscription : settings.getSubscriptions()) {
			socket.subscribe(subscription);
		}
		for (String inprocName : settings.getInprocNamesToBindTo()) {
			socket.bind(SocketSettings.getInprocAddress(inprocName));
		}
		for (int port : settings.getPortsToBindTo()) {
			socket.bind("tcp://*:" + port);
		}
	}
	
	public synchronized void updateSettings(final int socketId, SocketSettings socketSettings) {
		assertManagerActive();
		assertSocketExists(socketId);
		MessengerMutex<ZmqSocketMessenger> mutex = _socketMutexLookup.get(socketId);
		mutex.runAsOwner(new OwnerDelegate<Messenger>() {
			
			@Override
			public void asOwner(Messenger messenger) {
				ZmqSocketMessenger socketMessenger = (ZmqSocketMessenger) messenger;
				if (_settingsLookup.get(socketId).isBound()) {
					socketMessenger.getSocket().close();
				}
			}
		});
		_settingsLookup.put(socketId, socketSettings);
		newSocket(socketId);
	}
	
	public synchronized SocketSettings getSettings(int socketId) {
		assertManagerActive();
		assertSocketExists(socketId);
		return _settingsLookup.get(socketId);
	}
	
	public synchronized MessengerMutex<ZmqSocketMessenger> getSocketMutex(final int socketId) {
		assertManagerActive();
		assertSocketExists(socketId);
		return _socketMutexLookup.get(socketId);
	}
	
	public synchronized MultiMessengerMutex<ZmqSocketMessenger> createPollInSet(int... socketIds) {
		Objects.requireNonNull(socketIds);
		if (socketIds.length < 1) 
			throw new IllegalArgumentException("There must be at least one socket ID in the poll set.");
		ArrayList<MessengerMutex<ZmqSocketMessenger>> messengerMutexes = new ArrayList<>(socketIds.length);
		for (int i = 0; i < socketIds.length; i++) {
			messengerMutexes.add(i, getSocketMutex(socketIds[i]));
		}
		return MessengerMutex.createMultiMessengerMutex(new MultiMessengerFactory<ZmqSocketMessenger>() {

			@Override
			public Messenger create(Collection<ZmqSocketMessenger> messengers) {
				return new ZmqSocketSetMessenger(new AlphaSocketPollInSet(messengers.toArray(new ZmqSocketMessenger[messengers.size()])));
			}
			
		}, messengerMutexes);
	}
	
	public synchronized int connectSocket(final int socketId, final String address) {
		assertManagerActive();
		assertSocketExists(socketId);
		
		MessengerMutex<ZmqSocketMessenger> socketToken = _socketMutexLookup.get(socketId);
		socketToken.runAsOwner(new OwnerDelegate<Messenger>() {
			
			@Override
			public void asOwner(Messenger messenger) {
				ZmqSocketMessenger socketMessenger = (ZmqSocketMessenger) messenger;
				ZMQ.Socket socket = socketMessenger.getSocket();
				socket.connect(address);
			}
		});
		ConnectionsRecord connsRecord = _socketConnLookup.get(socketId);
		return connsRecord.addConnString(address);
	}
	
	public synchronized String disconnectSocket(final int socketId, final int connId) {
		assertManagerActive();
		assertSocketExists(socketId);
		final ConnectionsRecord connsRecord = _socketConnLookup.get(socketId);
		final String connString = connsRecord.getConnString(connId);
		if (connString != null) {
			MessengerMutex<ZmqSocketMessenger> token = _socketMutexLookup.get(socketId);
			token.runAsOwner(new OwnerDelegate<Messenger>() {

				@Override
				public void asOwner(Messenger messenger) {
					ZmqSocketMessenger socketMessenger = (ZmqSocketMessenger) messenger;
					socketMessenger.getSocket().disconnect(connString);
					connsRecord.removeConnString(connId);
				}
				
			});
		}
		return connString;
	}
	
	public synchronized void destroySocket(int socketId) {
		MessengerMutex<ZmqSocketMessenger> socketToken = getSocketMutex(socketId);
		try {
			socketToken.waitForRelease(60, TimeUnit.SECONDS);
			if (socketToken.isOwned()) {
				throw new RuntimeException(String.format("The thread closing the socket %d timed out.", socketId));
			}
		} catch (InterruptedException eInterrupted) {
			throw new RuntimeException(String.format("Thread was interrupted while closing socket %d", socketId));
		}

		try {
			socketToken.runAsOwner(new OwnerDelegate<Messenger>() {

				@Override
				public void asOwner(Messenger messenger) {
					ZmqSocketMessenger socketMessenger = (ZmqSocketMessenger) messenger;
					socketMessenger.getSocket().close();
				}
				
			});
		} finally {
			_socketMutexLookup.remove(socketId);
			_socketTypeLookup.remove(socketId);
		}
	}
	
	public synchronized void destroyAllSockets() {
		assertManagerActive();
		IntSet socketIdSet = new IntArraySet(_socketMutexLookup.keySet());
		for (int socketId : socketIdSet) {
			destroySocket(socketId);
		}
	}
	
	/**
	 * Closes the socket manager, releasing all resources.
	 */
	@Override
	public synchronized void close() {
		destroyAllSockets();
		_zmqContext.term();
		_isActive = false;
	}
	
	private void assertManagerActive() {
		if (!_isActive) throw new IllegalStateException("The socket manager has been closed.");
	}
	
	private void assertSocketExists(int socketId) {
		if (!_socketMutexLookup.containsKey(socketId))
			throw new IllegalArgumentException(String.format("No such socket with id %d", socketId));
	}
	
	private static class ConnectionsRecord {
		private final Int2ObjectMap<String> _connections = new Int2ObjectArrayMap<>();
		private int _nextConnId = 0;
		
		public String getConnString(int connId) {
			return _connections.get(connId);
		}
		
		public int addConnString(String connString) {
			int connId = _nextConnId++;
			_connections.put(connId, connString);
			return connId;
		}
		
		public Collection<String> getAllConnectionStrings() {
			return _connections.values();
		}
		
		public String removeConnString(int connId) {
			return _connections.remove(connId);
		}
	}
	
}