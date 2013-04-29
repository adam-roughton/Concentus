package com.adamroughton.concentus.messaging;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.messaging.SocketMutex.SocketSetFactory;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;

public class SocketManager implements Closeable {

	private static final int SOCKET_TIMEOUT = 1000;
	
	private final ZMQ.Context _zmqContext;
	private final Int2ObjectMap<ConnectionsRecord> _socketConnLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<SocketMutex> _socketMutexLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<SocketSettings> _settingsLookup = new Int2ObjectArrayMap<>();
	private final IntSet _boundSet = new IntArraySet();
	private final IntSet _connectedSet = new IntArraySet();
	private int _nextSocketId = 0;
	private boolean _isActive = true;
	
	public SocketManager() {
		this(1);
	}
	
	public SocketManager(int ioThreads) {
		_zmqContext = ZMQ.context(ioThreads);
	}
	
	/**
	 * Creates a new managed socket. The socket is not configured until
	 * a call to {@link SocketManager#bindBoundSockets()} or {@link SocketManager#connectSocket(int, String)}
	 * is made.
	 * @param socketType the ZMQ socket type
	 * @return the socketID which refers to the created socket
	 */
	public synchronized int create(final int socketType) {
		return create(socketType, SocketSettings.create());
	}
	
	/**
	 * Creates a new managed socket. The socket is not configured until
	 * a call to {@link SocketManager#bindBoundSockets()} or {@link SocketManager#connectSocket(int, String)}
	 * is made.
	 * @param socketType the ZMQ socket type
	 * @param socketSettings the settings to apply to the socket when opening
	 * @return the socketID which refers to the created socket
	 */
	public synchronized int create(final int socketType, SocketSettings socketSettings) {
		assertManagerActive();
		int socketId = _nextSocketId++;
		ZMQ.Socket socket = _zmqContext.socket(socketType);
		socket.setReceiveTimeOut(SOCKET_TIMEOUT);
		socket.setSendTimeOut(SOCKET_TIMEOUT);
		socket.setLinger(0);
		_socketMutexLookup.put(socketId, new SocketMutex(SocketPackage.create(socketId, socket)));
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
		_socketConnLookup.put(socketId, new ConnectionsRecord());
		return socketId;
	}
	
	public synchronized SocketSettings getSettings(final int socketId) {
		return _settingsLookup.get(socketId);
	}
	
	public synchronized void updateSettings(final int socketId, final SocketSettings socketSettings) {
		assertNotOpen(socketId);
		assertSocketExists(socketId);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
	}
	
	public synchronized SocketMutex getSocketMutex(final int socketId) {
		assertManagerActive();
		assertSocketExists(socketId);
		return _socketMutexLookup.get(socketId);
	}
	
	public synchronized Mutex<SocketPollInSet> createPollInSet(int... socketIds) {
		Objects.requireNonNull(socketIds);
		if (socketIds.length < 1) 
			throw new IllegalArgumentException("There must be at least one socket ID in the poll set.");
		SocketMutex first = getSocketMutex(socketIds[0]);
		SocketMutex[] additional = new SocketMutex[socketIds.length - 1];
		for (int i = 1; i < socketIds.length; i++) {
			additional[i - 1] = getSocketMutex(socketIds[i]);
		}
		return SocketMutex.createSetMutex(new SocketSetFactory<SocketPollInSet>() {

			@Override
			public SocketPollInSet create(SocketPackage... packages) {
				return new SocketPollInSet(_zmqContext, packages);
			}
			
		}, first, additional);
	}
	
	public synchronized void bindBoundSockets() {
		assertManagerActive();
		for (final int socketId : _socketMutexLookup.keySet()) {
			final SocketSettings settings = _settingsLookup.get(socketId);
			if (settings.isBound() && !_boundSet.contains(socketId)) {
				SocketMutex token = _socketMutexLookup.get(socketId);
				token.runAsOwner(new OwnerDelegate<SocketPackage>() {
					
					@Override
					public void asOwner(SocketPackage socketPackage) {
						settings.configureSocket(socketPackage);
						_boundSet.add(socketId);
					}
				});				
			}
		}
	}
	
	public synchronized int connectSocket(final int socketId, final String address) {
		assertManagerActive();
		assertSocketExists(socketId);
		
		SocketMutex socketToken = _socketMutexLookup.get(socketId);
		final SocketSettings settings = _settingsLookup.get(socketId);
		socketToken.runAsOwner(new OwnerDelegate<SocketPackage>() {
			
			@Override
			public void asOwner(SocketPackage socketPackage) {
				ZMQ.Socket socket = socketPackage.getSocket();
				
				// make sure the socket is bound first before connecting
				if (settings.isBound() && !_boundSet.contains(socketId)) {
					settings.configureSocket(socketPackage);
					_boundSet.add(socketId);
				}
				// apply configuration once
				if (!settings.isBound() && !_connectedSet.contains(socketId)) {
					settings.configureSocket(socketPackage);
					_connectedSet.add(socketId);
				}
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
			SocketMutex token = _socketMutexLookup.get(socketId);
			token.runAsOwner(new OwnerDelegate<SocketPackage>() {

				@Override
				public void asOwner(SocketPackage socketPackage) {
					socketPackage.getSocket().disconnect(connString);
					connsRecord.removeConnString(connId);
				}
				
			});
		}
		return connString;
	}
	
	public synchronized void closeSocket(int socketId) {
		SocketMutex socketToken = getSocketMutex(socketId);
		try {
			socketToken.waitForRelease(60, TimeUnit.SECONDS);
			if (socketToken.isOwned()) {
				throw new RuntimeException(String.format("The thread closing the socket %d timed out.", socketId));
			}
		} catch (InterruptedException eInterrupted) {
			throw new RuntimeException(String.format("Thread was interrupted while closing socket %d", socketId));
		}

		try {
			socketToken.runAsOwner(new OwnerDelegate<SocketPackage>() {

				@Override
				public void asOwner(SocketPackage socketPackage) {
					socketPackage.getSocket().close();
				}
				
			});
		} finally {
			_socketMutexLookup.remove(socketId);
			_connectedSet.remove(socketId);
			_boundSet.remove(socketId);
		}
	}
	
	public synchronized void closeManagedSockets() {
		assertManagerActive();
		IntSet openSocketIdSet = new IntArraySet();
		openSocketIdSet.addAll(_connectedSet);
		openSocketIdSet.addAll(_boundSet);
		for (int socketId : openSocketIdSet) {
			closeSocket(socketId);
		}
	}
	
	/**
	 * Closes the socket manager, releasing all resources.
	 */
	@Override
	public synchronized void close() {
		closeManagedSockets();
		_zmqContext.term();
		_isActive = false;
	}
	
	private void assertNotOpen(final int socketId) {
		if (_boundSet.contains(socketId) || _connectedSet.contains(socketId)) {
			throw new IllegalStateException(String.format("The socket associated with ID %d has already been opened.", socketId));
		}
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
		
		public String removeConnString(int connId) {
			return _connections.remove(connId);
		}
	}
	
}
