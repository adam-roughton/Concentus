package com.adamroughton.concentus.messaging.zmq;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.MessengerClosedException;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

final class ZmqSocketOperations {
	
	/**
	 * Sends segments from the given buffer using
	 * the provided header. All bounds and event validity checks
	 * are left to the caller. If endSegmentIndex is less than the last segment
	 * index, the socket will be left in a 'SNDMORE' state.
	 * 
	 * @param socket the socket to send on
	 * @param outgoingBuffer the buffer to send from
	 * @param header the outgoing event header
	 * @param startSegmentIndex the segment index to start sending from
	 * @param isBlocking whether the send operations should block
	 * @return the last successful segment index sent, or -1 if
	 * no segments could be sent
	 */
	public static int sendSegments(
			ZMQ.Socket socket,
			ArrayBackedResizingBuffer outgoingBuffer, 
			OutgoingEventHeader header,
			int startSegmentIndex,
			int endSegmentIndex,
			boolean isBlocking) {
		if (startSegmentIndex < 0)
			throw new IllegalArgumentException("The start segment index cannot be negative");
		if (endSegmentIndex < startSegmentIndex)
			throw new IllegalArgumentException("The end segment index must be greater " +
					"than or equal to the start segment index");
		
		int zmqFlags = isBlocking? 0 : ZMQ.NOBLOCK;
		
		int segmentCount = header.getSegmentCount();
		int segmentIndex = startSegmentIndex;
		
		byte[] msgBytes = outgoingBuffer.getBuffer();
		
		for (;segmentIndex <= endSegmentIndex; segmentIndex++) {
			int segmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentIndex);
			int offset = EventHeader.getSegmentOffset(segmentMetaData);
			int length = EventHeader.getSegmentLength(segmentMetaData);
			int flags = zmqFlags | ((segmentIndex < segmentCount - 1)? ZMQ.SNDMORE : 0);
			if (!ZmqSocketOperations.doSend(socket, msgBytes, offset, length, flags)) {
				break;
			}
		}
		return segmentIndex - 1;
	}
	
	public static boolean doSend(ZMQ.Socket socket, byte[] outgoingBuffer, int offset, int length, int sendFlags) {
		try {
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
		} catch (ZMQException eZmq) {
			// check that the socket hasn't just been closed
			if (eZmq.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
				throw MessengerClosedException.INSTANCE;
			} else {
				throw eZmq;
			}
		}
	}
	
	public static int doRecv(ZMQ.Socket socket, byte[] eventBuffer, int offset, int length, boolean isBlocking) {
		int safeLength = Math.min(eventBuffer.length - offset, length);
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
					recvdAmount = socket.recv(eventBuffer, offset, safeLength, recvFlag);
				}
			} else {
				recvdAmount = socket.recv(eventBuffer, offset, safeLength, recvFlag);
				recvdAmount = recvdAmount == -1? 0 : recvdAmount;
			}
			return recvdAmount;
		} catch (ZMQException eZmq) {
			// if we get an exception, there was an error besides EAGAIN (jzmq handles this for us)
			// check that the socket hasn't just been closed
			if (eZmq.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
				throw MessengerClosedException.INSTANCE;
			} else {
				return -1;
			}
		}
	}

}
