package com.adamroughton.consentus.messaging;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Closeable;
import java.util.Objects;

import org.zeromq.ZMQ;

public class SocketManager implements Closeable {

	private final ZMQ.Context _zmqContext;
	private final Int2ObjectMap<ZMQ.Socket> _socketLookup = new Int2ObjectArrayMap<>();
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
		_socketLookup.put(socketId, socket);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
		return socketId;
	}
	
	public synchronized SocketSettings getSettings(final int socketId) {
		return _settingsLookup.get(socketId);
	}
	
	public synchronized void updateSettings(final int socketId, final SocketSettings socketSettings) {
		assertNotOpen(socketId);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
	}
	
	public synchronized ZMQ.Socket getSocket(final int socketId) {
		return _socketLookup.get(socketId);
	}
	
	public synchronized SocketPackage createSocketPackage(final int socketId) {
		assertManagerActive();
		ZMQ.Socket socket = _socketLookup.get(socketId);
		return SocketPackage.create(socket)
					 .setSocketId(socketId);
	}
	
	public synchronized SocketPollInSet createPollInSet(SocketPackage... socketPackages) {
		assertManagerActive();
		return new SocketPollInSet(_zmqContext, socketPackages);
	}
	
	public synchronized void bindBoundSockets() {
		assertManagerActive();
		for (int socketId : _socketLookup.keySet()) {
			SocketSettings settings = _settingsLookup.get(socketId);
			if (settings.isBound() && !_boundSet.contains(socketId)) {
				settings.configureSocket(_socketLookup.get(socketId));
				_boundSet.add(socketId);
			}
		}
	}
	
	public synchronized void connectSocket(int socketId, String address) {
		assertManagerActive();
		ZMQ.Socket socket = _socketLookup.get(socketId);
		SocketSettings settings = _settingsLookup.get(socketId);
		// make sure the socket is bound first before connecting
		if (settings.isBound() && !_boundSet.contains(socketId)) {
			settings.configureSocket(socket);
			_boundSet.add(socketId);
		}
		// apply configuration once
		if (!settings.isBound() && !_connectedSet.contains(socketId)) {
			settings.configureSocket(socket);
			_connectedSet.add(socketId);
		}
		socket.connect(address);
	}
	
	public synchronized void closeManagedSockets() {
		assertManagerActive();
		IntSet openSocketIdSet = new IntArraySet();
		openSocketIdSet.addAll(_connectedSet);
		openSocketIdSet.addAll(_boundSet);
		for (int socketId : openSocketIdSet) {
			ZMQ.Socket socket = _socketLookup.get(socketId);
			try {
				socket.close();
			} finally {
				_socketLookup.remove(socketId);
				_connectedSet.remove(socketId);
				_boundSet.remove(socketId);
			}
		}
	}
	
	/**
	 * Closes the socket manager, releasing all resources.
	 */
	@Override
	public synchronized void close() {
		_isActive = false;
		closeManagedSockets();
		_zmqContext.term();
	}
	
	private void assertNotOpen(final int socketId) {
		if (_boundSet.contains(socketId) || _connectedSet.contains(socketId)) {
			throw new IllegalStateException(String.format("The socket associated with ID %d has already been opened.", socketId));
		}
	}
	
	private void assertManagerActive() {
		if (!_isActive) throw new IllegalStateException("The socket manager has been closed.");
	}
	
}
