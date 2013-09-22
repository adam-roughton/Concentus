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
import java.util.concurrent.TimeoutException;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ArrayBackedResizingBufferFactory;
import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerBridge;
import com.adamroughton.concentus.messaging.MessengerBridge.BridgeDelegate;
import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerFactory;
import com.adamroughton.concentus.messaging.MessengerMutex.MultiMessengerMutex;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;

public final class ArrayBackedZmqSocketManagerImpl implements ZmqSocketManager<ArrayBackedResizingBuffer> {

	private static final int SOCKET_TIMEOUT = 1000;
	
	private final Clock _clock;
	private final ZMQ.Context _zmqContext;
	private final Int2IntMap _socketTypeLookup = new Int2IntArrayMap();
	private final Int2ObjectMap<ConnectionsRecord> _socketConnLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger>> _socketMutexLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<SocketSettings> _settingsLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<MessengerMutex<ArrayBackedResizingBuffer, ZmqDealerSetSocketMessenger>> _dealerSetMessengers = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<IdentityLookup> _socketToIdentityLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<IntSet> _extPortLookup = new Int2ObjectArrayMap<>();
	
	private int _nextSocketId = 0;
	private boolean _isActive = true;
	
	public ArrayBackedZmqSocketManagerImpl(Clock clock) {
		this(clock, 1);
	}
	
	public ArrayBackedZmqSocketManagerImpl(Clock clock, int ioThreads) {
		_clock = Objects.requireNonNull(clock);
		_zmqContext = ZMQ.context(ioThreads);
	}
	
	@Override
	public BufferFactory<ArrayBackedResizingBuffer> getBufferFactory() {
		return new ArrayBackedResizingBufferFactory();
	}
	
	@Override
	public MessageQueueFactory<ArrayBackedResizingBuffer> newMessageQueueFactory(
			EventQueueFactory eventQueueFactory) {
		return new MessageQueueFactory<>(eventQueueFactory, new ArrayBackedResizingBufferFactory());
	}
	
	/**
	 * Creates a new managed socket. The socket is not opened until
	 * a call to {@link ZmqSocketManager#updateSettings(int, SocketSettings)} is made
	 * with a {@link SocketSettings} object that includes a port to bind to; or 
	 * a call to {@link ZmqSocketManager#connectSocket(int, String)} is made.
	 * @param socketType the ZMQ socket type
	 * @param name a name for the socket
	 * @return the socketID which refers to the created socket
	 */
	public synchronized int create(int socketType, String name) {
		return create(socketType, SocketSettings.create(), name);
	}
	
	public synchronized int create(int socketType, SocketSettings socketSettings, String name) {
		assertManagerActive();
		int socketId = _nextSocketId++;
		_socketTypeLookup.put(socketId, socketType);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
		_socketConnLookup.put(socketId, new ConnectionsRecord());
		
		if (socketType == ZmqSocketManager.DEALER_SET) {
			String recvPairAddress = socketSettings.getRecvPairAddress();
			if (recvPairAddress == null) {
				throw new IllegalArgumentException("The socket settings had no recv pair address defined for the dealer set socket");
			}
			ZmqDealerSetSocketMessenger dealerSetMessenger = 
					new ZmqDealerSetSocketMessenger(recvPairAddress, _zmqContext, _clock, name);
			_dealerSetMessengers.put(socketId, new MessengerMutex<>(dealerSetMessenger));
			_socketToIdentityLookup.put(socketId, dealerSetMessenger.getIdentityLookup());
		} else {
			newSocket(socketId, name);
		}
		return socketId;
	}
	
	private void newSocket(int socketId, String name) {
		int socketType = _socketTypeLookup.get(socketId);
		SocketSettings socketSettings = _settingsLookup.get(socketId);
		ZMQ.Socket socket = _zmqContext.socket(socketType);
		socket.setReceiveTimeOut(SOCKET_TIMEOUT);
		socket.setSendTimeOut(SOCKET_TIMEOUT);
		socket.setLinger(0);
		ZmqSocketMessenger messenger = new ZmqStandardSocketMessenger(socketId, name, socket);
		_socketMutexLookup.put(socketId, new MessengerMutex<>(messenger));
		configureSocket(socketId, socket, socketSettings);
		for (String connectionString : _socketConnLookup.get(socketId).getAllConnectionStrings()) {
			socket.connect(connectionString);
		}
	}
	
	private void configureSocket(int socketId, ZMQ.Socket socket, SocketSettings settings) {
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
			if (port == -1) {
				port = socket.bindToRandomPort("tcp://*");
			} else {
				socket.bind("tcp://*:" + port);
			}
			IntSet portSet;
			if (!_extPortLookup.containsKey(socketId)) {
				portSet = new IntArraySet();
				_extPortLookup.put(socketId, portSet);
			} else {
				portSet = _extPortLookup.get(socketId);
			}
			portSet.add(port);
		}
	}
	
	public synchronized int[] getBoundPorts(int socketId) {
		assertManagerActive();
		if (_extPortLookup.containsKey(socketId)) {
			return _extPortLookup.get(socketId).toIntArray();
		} else {
			return new int[0];
		}
	}
	
	public synchronized int getBoundPort(int socketId) {
		int[] boundPorts = getBoundPorts(socketId);
		if (boundPorts.length < 1) 
			throw new RuntimeException("No bound port for socket ID " + socketId);
		return boundPorts[0];
	}
	
	public synchronized void updateSettings(final int socketId, SocketSettings socketSettings) {
		assertManagerActive();
		assertSocketExists(socketId);
		MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger> mutex = _socketMutexLookup.get(socketId);
		final String[] nameContainer = new String[1];
		mutex.directRunAsOwner(new OwnerDelegate<ZmqSocketMessenger>() {
			
			@Override
			public void asOwner(ZmqSocketMessenger messenger) {
				ZmqSocketMessenger socketMessenger = (ZmqSocketMessenger) messenger;
				nameContainer[0] = messenger.name();
				if (_settingsLookup.get(socketId).isBound()) {
					socketMessenger.getSocket().close();
				}
			}
		});
		_settingsLookup.put(socketId, socketSettings);
		_extPortLookup.remove(socketId);
		newSocket(socketId, nameContainer[0]);
	}
	
	public synchronized SocketSettings getSettings(int socketId) {
		assertManagerActive();
		assertSocketExists(socketId);
		return _settingsLookup.get(socketId);
	}
	
	public synchronized Mutex<Messenger<ArrayBackedResizingBuffer>> getSocketMutex(final int socketId) {
		assertManagerActive();
		if (_dealerSetMessengers.containsKey(socketId)) {
			return _dealerSetMessengers.get(socketId);
		} else {
			return getSocketMutexInternal(socketId);
		}
	}
	
	private MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger> getSocketMutexInternal(final int socketId) {
		assertSocketExists(socketId);
		return _socketMutexLookup.get(socketId);
	}
	
	public synchronized MultiMessengerMutex<ArrayBackedResizingBuffer, 
			Messenger<ArrayBackedResizingBuffer>, 
			ZmqSocketMessenger> createPollInSet(int... socketIds) {
		Objects.requireNonNull(socketIds);
		if (socketIds.length < 1) 
			throw new IllegalArgumentException("There must be at least one socket ID in the poll set.");
		ArrayList<MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger>> messengerMutexes = new ArrayList<>(socketIds.length);
		for (int i = 0; i < socketIds.length; i++) {
			messengerMutexes.add(i, getSocketMutexInternal(socketIds[i]));
		}
		return MessengerMutex.createMultiMessengerMutex(new MultiMessengerFactory<ArrayBackedResizingBuffer, 
				Messenger<ArrayBackedResizingBuffer>, ZmqSocketMessenger>() {

			@Override
			public Messenger<ArrayBackedResizingBuffer> create(Collection<ZmqSocketMessenger> messengers) {
				return new ZmqSocketSetMessenger(new AlphaSocketPollInSet(messengers.toArray(new ZmqSocketMessenger[messengers.size()])));
			}
			
		}, messengerMutexes);
	}
	
	@Override
	public MessengerBridge<ArrayBackedResizingBuffer> newBridge(
			int frontendSocketId, int backendSocketId,
			BridgeDelegate<ArrayBackedResizingBuffer> bridgeDelegate, 
			IncomingEventHeader frontendHeader, OutgoingEventHeader backendHeader) {
		return newBridge(frontendSocketId, backendSocketId, bridgeDelegate, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, frontendHeader, backendHeader);
	}
	
	@Override
	public MessengerBridge<ArrayBackedResizingBuffer> newBridge(
			int frontendSocketId, int backendSocketId,
			BridgeDelegate<ArrayBackedResizingBuffer> bridgeDelegate, int defaultBufferSize, 
			IncomingEventHeader frontendHeader, OutgoingEventHeader backendHeader) {
		assertManagerActive();
		Mutex<Messenger<ArrayBackedResizingBuffer>> frontendMessenger = getSocketMutex(frontendSocketId);
		assertAvailable(frontendSocketId, frontendMessenger);
		
		Mutex<Messenger<ArrayBackedResizingBuffer>> backendMessenger = getSocketMutex(backendSocketId);
		assertAvailable(backendSocketId, backendMessenger);
		
		return new MessengerBridge<>(bridgeDelegate, defaultBufferSize, getBufferFactory(), 
				frontendMessenger, backendMessenger, frontendHeader, backendHeader);
	}
	
	private void assertAvailable(int socketId, Mutex<Messenger<ArrayBackedResizingBuffer>> messengerMutex) {
		if (messengerMutex == null) {
			throw new IllegalArgumentException("The socket ID " + socketId + 
					" was not found in this socket manager.");
		} else if (messengerMutex.isOwned()) {
			throw new IllegalArgumentException("The socket ID " + socketId + 
					" is already in use by another component.");
		}
	}
	
	@Override
	public int connectSocket(int socketId, ServiceEndpoint endpoint) {
		return connectSocket(socketId, String.format("tcp://%s:%d", endpoint.ipAddress(), endpoint.port()));
	}
	
	public synchronized int connectSocket(final int socketId, final String address) {
		assertManagerActive();
		
		if (_dealerSetMessengers.containsKey(socketId)) {
			MessengerMutex<ArrayBackedResizingBuffer, ZmqDealerSetSocketMessenger> messengerToken = _dealerSetMessengers.get(socketId);
			messengerToken.directRunAsOwner(new OwnerDelegate<ZmqDealerSetSocketMessenger>() {

				@Override
				public void asOwner(ZmqDealerSetSocketMessenger messenger) {
					messenger.connect(address);
				}
				
			});
		} else {
			assertSocketExists(socketId);
			
			MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger> socketToken = _socketMutexLookup.get(socketId);
			socketToken.directRunAsOwner(new OwnerDelegate<ZmqSocketMessenger>() {
				
				@Override
				public void asOwner(ZmqSocketMessenger messenger) {
					ZMQ.Socket socket = messenger.getSocket();
					socket.connect(address);
				}
			});
		}
		ConnectionsRecord connsRecord = _socketConnLookup.get(socketId);
		return connsRecord.addConnString(address);
	}
	
	public synchronized String disconnectSocket(final int socketId, final int connId) {
		assertManagerActive();
		final ConnectionsRecord connsRecord = _socketConnLookup.get(socketId);
		final String connString = connsRecord.getConnString(connId);
		
		if (_dealerSetMessengers.containsKey(socketId)) {
			if (connString != null) {
				MessengerMutex<ArrayBackedResizingBuffer, ZmqDealerSetSocketMessenger> token = _dealerSetMessengers.get(socketId);
				token.directRunAsOwner(new OwnerDelegate<ZmqDealerSetSocketMessenger>() {
	
					@Override
					public void asOwner(ZmqDealerSetSocketMessenger messenger) {
						messenger.disconnect(connString);
						connsRecord.removeConnString(connId);
					}
					
				});
			}
		} else {
			assertSocketExists(socketId);
			if (connString != null) {
				MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger> token = _socketMutexLookup.get(socketId);
				token.directRunAsOwner(new OwnerDelegate<ZmqSocketMessenger>() {
	
					@Override
					public void asOwner(ZmqSocketMessenger messenger) {
						messenger.getSocket().disconnect(connString);
						connsRecord.removeConnString(connId);
					}
					
				});
			}
		}
		return connString;
	}
	
	public synchronized void destroySocket(int socketId) {
		if (_dealerSetMessengers.containsKey(socketId)) {
			MessengerMutex<ArrayBackedResizingBuffer, ZmqDealerSetSocketMessenger> socketToken = _dealerSetMessengers.get(socketId);
			waitOnSocketToken(socketToken, socketId);
			try {
				socketToken.directRunAsOwner(new OwnerDelegate<ZmqDealerSetSocketMessenger>() {

					@Override
					public void asOwner(ZmqDealerSetSocketMessenger messenger) {
						messenger.close();
					}
					
				});
			} finally {
				_dealerSetMessengers.remove(socketId);
			}
		} else {
			MessengerMutex<ArrayBackedResizingBuffer, ZmqSocketMessenger> socketToken = getSocketMutexInternal(socketId);
			waitOnSocketToken(socketToken, socketId);
			try {
				socketToken.directRunAsOwner(new OwnerDelegate<ZmqSocketMessenger>() {

					@Override
					public void asOwner(ZmqSocketMessenger socketMessenger) {
						socketMessenger.getSocket().close();
					}
					
				});
			} finally {
				_socketMutexLookup.remove(socketId);
				_socketTypeLookup.remove(socketId);
				_extPortLookup.remove(socketId);
			}
		}
	}
	
	private void waitOnSocketToken(Mutex<?> socketToken, int socketId) {
		try {
			socketToken.waitForRelease(60, TimeUnit.SECONDS);
			if (socketToken.isOwned()) {
				throw new RuntimeException(String.format("The thread closing the socket %d timed out.", socketId));
			}
		} catch (InterruptedException eInterrupted) {
			throw new RuntimeException(String.format("Thread was interrupted while closing socket %d", socketId));
		}

	}
	
	public synchronized void destroyAllSockets() {
		assertManagerActive();
		IntSet socketIdSet = new IntArraySet(_socketMutexLookup.keySet());
		socketIdSet.addAll(_dealerSetMessengers.keySet());
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
	
	@Override
	public SocketIdentity resolveIdentity(int socketId,
			ServiceEndpoint endpoint, long timeout, TimeUnit timeUnit)
			throws InterruptedException, TimeoutException,
			UnsupportedOperationException {
		return resolveIdentity(socketId, String.format("tcp://%s:%d", endpoint.ipAddress(), 
				endpoint.port()), timeout, timeUnit);
	}

	@Override
	public SocketIdentity resolveIdentity(int socketId,
			String connectionString, long timeout, TimeUnit timeUnit)
			throws InterruptedException, TimeoutException,
			UnsupportedOperationException {
		assertManagerActive();
		if (_socketToIdentityLookup.containsKey(socketId)) {
			return _socketToIdentityLookup.get(socketId).resolveIdentity(connectionString, timeout, timeUnit);
		} else {
			throw new UnsupportedOperationException("The socketID " + socketId + " does not support address resolution.");
		}
	}
	
}