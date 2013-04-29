package com.adamroughton.concentus.messaging;

import java.util.Objects;

import org.zeromq.ZMQ;

public class SocketPackage {

	private final ZMQ.Socket _socket;
	private final int _socketId;
	
	private SocketPackage(int socketId, ZMQ.Socket socket) {
		_socketId = socketId;
		_socket = Objects.requireNonNull(socket);
	}
	
	public static SocketPackage create(int socketId, ZMQ.Socket socket) {
		return new SocketPackage(socketId, socket);
	}
	
	/**
	 * The identifier that will be written in to an attached
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
	
	public SocketPackage updateSocketId(int socketId) {
		return new SocketPackage(socketId, _socket);
	}
	
}
