package com.adamroughton.concentus.messaging;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.util.StatefulRunnable;
import com.adamroughton.concentus.util.Util;
import com.adamroughton.concentus.util.StatefulRunnable.State;

public class SocketManager implements Closeable {

	private static final Logger LOG = Logger.getLogger(SocketManager.class.getName());
	private static final int SOCKET_TIMEOUT = 1000;
	
	private final ExecutorService _proxyExecutor = Executors.newCachedThreadPool();
	
	private final ZMQ.Context _zmqContext;
	private final List<StatefulRunnable<? extends Runnable>> _proxies = new ArrayList<>();
	private final Int2ObjectMap<ConnectionsRecord> _socketConnLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<StatefulRunnable<?>> _socketDepLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<ZMQ.Socket> _socketLookup = new Int2ObjectArrayMap<>();
	private final Int2ObjectMap<SocketSettings> _settingsLookup = new Int2ObjectArrayMap<>();
	private final IntSet _boundSet = new IntArraySet();
	private final IntSet _connectedSet = new IntArraySet();
	private final IntSet _proxyParticipantSet = new IntArraySet();
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
		_socketLookup.put(socketId, socket);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
		_socketConnLookup.put(socketId, new ConnectionsRecord());
		return socketId;
	}
	
	public synchronized void addDependency(final int socketId, StatefulRunnable<?> runnable) {
		assertManagerActive();
		assertSocketExists(socketId);
		_socketDepLookup.put(socketId, runnable);
	}
	
	public synchronized SocketSettings getSettings(final int socketId) {
		return _settingsLookup.get(socketId);
	}
	
	public synchronized void updateSettings(final int socketId, final SocketSettings socketSettings) {
		assertNotOpen(socketId);
		assertSocketExists(socketId);
		_settingsLookup.put(socketId, Objects.requireNonNull(socketSettings));
	}
	
	public synchronized ZMQ.Socket getSocket(final int socketId) {
		return _socketLookup.get(socketId);
	}
	
	public synchronized SocketPackage createSocketPackage(final int socketId) {
		assertManagerActive();
		assertSocketExists(socketId);
		ZMQ.Socket socket = _socketLookup.get(socketId);
		return SocketPackage.create(socket)
					 .setSocketId(socketId);
	}
	
	public synchronized SocketPollInSet createPollInSet(SocketPackage... socketPackages) {
		assertManagerActive();
		return new SocketPollInSet(_zmqContext, socketPackages);
	}
	
	public synchronized SocketPollInSet createPollInSet(int... socketIds) {
		SocketPackage[] socketPackages = new SocketPackage[socketIds.length];
		for (int i = 0; i < socketIds.length; i++) {
			socketPackages[i] = createSocketPackage(socketIds[i]);
		}
		return createPollInSet(socketPackages);
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
	
	public synchronized int connectSocket(int socketId, String address) {
		assertManagerActive();
		assertSocketExists(socketId);
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
		ConnectionsRecord connsRecord = _socketConnLookup.get(socketId);
		return connsRecord.addConnString(address);
	}
	
	public synchronized String disconnectSocket(int socketId, int connId) {
		assertManagerActive();
		assertSocketExists(socketId);
		ConnectionsRecord connsRecord = _socketConnLookup.get(socketId);
		String connString = connsRecord.getConnString(connId);
		if (connString != null) {
			_socketLookup.get(socketId).disconnect(connString);
			connsRecord.removeConnString(connId);
		}
		return connString;
	}
	
	/**
	 * Creates a new proxy that bridges between the front end socket and the back end socket.
	 * Each socket should already be connected and bound before the proxy is created.
	 * @param frontendSocketId
	 * @param backendSocketId
	 * @see ZMQ#proxy(org.zeromq.ZMQ.Socket, org.zeromq.ZMQ.Socket, org.zeromq.ZMQ.Socket)
	 */
	public synchronized void createProxy(int frontendSocketId, int backendSocketId) {
		assertManagerActive();
		assertSocketExists(frontendSocketId);
		assertSocketExists(backendSocketId);
		
		// ensure that the sockets are at least bound
		for (int socketId : Arrays.asList(frontendSocketId, backendSocketId)) {
			SocketSettings settings = _settingsLookup.get(socketId);
			// make sure the socket is bound first before connecting
			if (settings.isBound() && !_boundSet.contains(socketId)) {
				throw new IllegalStateException(String.format("The socket must be bound before it can be used in a proxy (socketId = %d)", socketId));
			}
		}
		
		final ZMQ.Socket frontendSocket = _socketLookup.get(frontendSocketId);
		final ZMQ.Socket backendSocket = _socketLookup.get(backendSocketId);
		
		StatefulRunnable<? extends Runnable> proxy = Util.asStateful(new Runnable() {

			@Override
			public void run() {
				ZMQ.proxy(frontendSocket, backendSocket, null);
			}
			
		});
		_proxies.add(proxy);
		_proxyParticipantSet.add(frontendSocketId);
		_proxyParticipantSet.add(backendSocketId);
		_proxyExecutor.execute(proxy);
	}
	
	public synchronized void closeSocket(int socketId) {
		boolean shouldClose = true;
		if (_proxyParticipantSet.contains(socketId)) {
			shouldClose = false;
		} else if (_socketDepLookup.containsKey(socketId)) {
			StatefulRunnable<?> dep = _socketDepLookup.get(socketId);
			try {
				dep.waitForState(State.STOPPED, SOCKET_TIMEOUT * 2, TimeUnit.MILLISECONDS);
				if (dep.getState() != State.STOPPED)
					shouldClose = false;
			} catch (InterruptedException eInterrupted) {
				shouldClose = false;
				Thread.currentThread().interrupt();
			}
		}
		try {
			if (shouldClose) {
				ZMQ.Socket socket = _socketLookup.get(socketId);
				socket.close();
			}
		} finally {
			_socketLookup.remove(socketId);
			_connectedSet.remove(socketId);
			_boundSet.remove(socketId);
			_socketDepLookup.remove(socketId);
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
		for (StatefulRunnable<? extends Runnable> proxy : _proxies) {
			try {
				proxy.waitForState(State.STOPPED, 30, TimeUnit.SECONDS);
				if (proxy.getState() != State.STOPPED) {
					LOG.warning("Unable to stop a proxy.");
				}
			} catch (InterruptedException eInterrupted) {
				LOG.warning("Interrupted while stopping a proxy.");
				Thread.currentThread().interrupt();
			}
		}
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
		if (!_socketLookup.containsKey(socketId))
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
