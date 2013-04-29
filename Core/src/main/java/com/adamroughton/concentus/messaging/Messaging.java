package com.adamroughton.concentus.messaging;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class Messaging {

	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if the socket is ready.
	 * @param socketPackage the socket (plus additional settings) to send the event on
	 * @param outgoingBuffer the buffer to send from
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was sent.
	 */
	public static boolean send(final SocketPackage socketPackage, 
			byte[] outgoingBuffer,
			OutgoingEventHeader header,
			boolean isBlocking) {
		return send(socketPackage.getSocket(), outgoingBuffer, header, isBlocking);
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, selecting the
	 * target socket using the socket ID in the buffer's header, and the provided
	 * set of sockets.
	 * @param socketSet the collection of possible sockets that can be identified
	 * in the buffer header
	 * @param outgoingBuffer the buffer to send from
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was sent
	 * @throws RuntimeException if a socket ID cannot be found in the socket set
	 */
	public static boolean send(final SocketSet socketSet,
			byte[] outgoingBuffer,
			MultiSocketOutgoingEventHeader header,
			boolean isBlocking) {
		int socketId = header.getTargetSocketId(outgoingBuffer);
		SocketPackage socketPackage = socketSet.getSocket(socketId);
		if (socketPackage == null)
			throw new RuntimeException(String.format("The socket associated with ID %d was not found in the socket set.", socketId));
		return send(socketPackage, outgoingBuffer, header, isBlocking);
	}
	
	/**
	 * Attempts to send a pending event from the outgoing buffer, succeeding
	 * only if the socket is ready.
	 * 
	 * @param socket the socket to send the event on
	 * @param outgoingBuffer the buffer to send from
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return {@code true} if the complete event was sent, {@code false} otherwise
	 */
	public static boolean send(final ZMQ.Socket socket,
			byte[] outgoingBuffer,
			OutgoingEventHeader header,
			boolean isBlocking) {
		// only send if the event is valid
		if (!header.isValid(outgoingBuffer)) return false;	
		int zmqFlags = isBlocking? 0 : ZMQ.NOBLOCK;
		
		// check event bounds
		int segmentCount = header.getSegmentCount();
		int lastSegmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentCount - 1);
		int lastSegmentOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		int lastSegmentLength = EventHeader.getSegmentLength(lastSegmentMetaData);
		int requiredLength = lastSegmentOffset + lastSegmentLength;
		if (requiredLength > outgoingBuffer.length)
			throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
					outgoingBuffer.length, requiredLength));
		
		int segmentIndex;
		if (header.isPartiallySent(outgoingBuffer)) {
			segmentIndex = header.getNextSegmentToSend(outgoingBuffer);
		} else {
			segmentIndex = 0;
		}
		for (;segmentIndex < segmentCount; segmentIndex++) {
			int segmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentIndex);
			int offset = EventHeader.getSegmentOffset(segmentMetaData);
			int length = EventHeader.getSegmentLength(segmentMetaData);
			int flags = zmqFlags | ((segmentIndex < segmentCount - 1)? ZMQ.SNDMORE : 0);
			if (!doSend(socket, outgoingBuffer, offset, length, flags)) {
				header.setNextSegmentToSend(outgoingBuffer, segmentIndex);
				header.setIsPartiallySent(outgoingBuffer, true);
				return false;
			}
		}
		return true;
	}
	
	private static boolean doSend(ZMQ.Socket socket, byte[] outgoingBuffer, int offset, int length, int sendFlags) {
		if ((sendFlags | ZMQ.NOBLOCK) != sendFlags) {
			/**
			 * blocking call:
			 * the socket should have a send timeout set, and so we anticipate
			 * that the send call might return false (EAGAIN). We keep retrying
			 * until the message has been sent, or the thread has been interrupted.
			 */
			boolean hasSent = false;
			while (!hasSent) {
				if (Thread.interrupted()) {
					Thread.currentThread().interrupt();
					return false;
				}
				hasSent = socket.send(outgoingBuffer, offset, length, sendFlags);
			}
			return true;
		} else {
			return socket.send(outgoingBuffer, offset, length, sendFlags);
		}
	}
	
	/**
	 * Receives an event on the given socket, filling the given
	 * event buffer as per the given message parts policy.
	 * @param socketPackage the socket (plus additional settings) to receive on
	 * @param eventBuffer the buffer to place the event in
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was placed in the buffer
	 */
	public static boolean recv(final SocketPackage socketPackage, 
			final byte[] eventBuffer,
			final IncomingEventHeader header,
			final boolean isBlocking) {
		return recv(socketPackage.getSocket(), 
				socketPackage.getSocketId(), 
				eventBuffer,
				header,
				isBlocking);
	}
	
	/**
	 * Attempts to receive an event on one of the sockets in the 
	 * poll set.
	 * @param socketPollInSet the set of sockets to poll for events
	 * on
	 * @param eventBuffer the buffer to place the event in
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was placed in the buffer
	 */
	public static boolean recv(final SocketPollInSet socketPollInSet,
			final byte[] eventBuffer,
			final IncomingEventHeader header,
			final boolean isBlocking) {
		SocketPackage readyPackage;
		if (isBlocking) {
			try {
				readyPackage = socketPollInSet.poll();
			} catch (InterruptedException eInterrupted) {
				Thread.currentThread().interrupt();
				return false;
			}
		} else {
			readyPackage = socketPollInSet.pollNoBlock();
		}
		if (readyPackage != null) {
			return recv(readyPackage, eventBuffer, header, isBlocking);
		} else {
			return false;
		}
	}
	
	/**
	 * Receives an event on the given socket, filling the given
	 * event buffer as per the given message parts policy.
	 * @param socket the socket to receive on
	 * @param socketId the socket ID to write into the header of incoming messages
	 * @param eventBuffer the buffer to place the event in
	 * @param header the header associated with the buffer
	 * @param isBlocking flag signalling whether the operation should block until complete,
	 * or immediately return regardless of success. {@code true} if the call should block, 
	 * {@code false} otherwise
	 * @return whether an event was placed in the buffer
	 */
	public static boolean recv(final ZMQ.Socket socket,
			int socketId,
			final byte[] eventBuffer,
			IncomingEventHeader header,
			boolean isBlocking) {			
		int cursor = header.getEventOffset();
		int expectedSegmentCount = header.getSegmentCount();
		int segmentIndex = 0;
		boolean isValid = true;
		
		do {
			if (segmentIndex > expectedSegmentCount || cursor >= eventBuffer.length) {
				isValid = false;
			}
			
			if (isValid) {
				int recvdAmount = doRecv(socket, eventBuffer, cursor, eventBuffer.length - cursor, isBlocking);
				if (segmentIndex == 0 && recvdAmount == 0) {
					// no message ready
					return false;
				} else if (recvdAmount == -1) {
					isValid = false;
				} else {
					header.setSegmentMetaData(eventBuffer, segmentIndex, cursor, recvdAmount);					
					cursor += recvdAmount;
					segmentIndex++;
				}
			} else {
				// absorb remaining segments
				socket.recv();
			}
		} while (socket.hasReceiveMore());
		
		header.setIsValid(eventBuffer, isValid);
		header.setSocketId(eventBuffer, socketId);
		return true;
	}
	
	private static int doRecv(ZMQ.Socket socket, byte[] eventBuffer, int offset, int length, boolean isBlocking) {
		try {
			int recvFlag = isBlocking? 0 : ZMQ.NOBLOCK;
			
			int recvdAmount;
			if (isBlocking) {
				
				/**
				 * blocking call:
				 * the socket should have a send timeout set, and so we anticipate
				 * that the recv call might return -1 (EAGAIN). We keep retrying
				 * until a message has been received, or the thread has been interrupted.
				 */
				recvdAmount = -1;
				// while still EAGAIN
				while (recvdAmount == -1) {
					if (Thread.interrupted()) {
						Thread.currentThread().interrupt();
						return 0;
					}
					recvdAmount = socket.recv(eventBuffer, offset, eventBuffer.length - offset, recvFlag);
				}
			} else {
				recvdAmount = socket.recv(eventBuffer, offset, eventBuffer.length - offset, recvFlag);
				recvdAmount = recvdAmount == -1? 0 : recvdAmount;
			}
			return recvdAmount;
		} catch (ZMQException eZmq) {
			// if we get an exception, there was an error besides EAGAIN
			return -1;
		}
	}
	
}
