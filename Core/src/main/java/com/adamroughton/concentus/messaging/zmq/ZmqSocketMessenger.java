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

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerClosedException;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public final class ZmqSocketMessenger implements Messenger {

	private final int _socketId;
	private final ZMQ.Socket _socket;
	private final Clock _clock;
	
	public ZmqSocketMessenger(int socketId, ZMQ.Socket socket, Clock clock) {
		_socketId = socketId;
		_socket = Objects.requireNonNull(socket);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public boolean send(byte[] outgoingBuffer, 
			OutgoingEventHeader header,
			boolean isBlocking) {
		// only send if the event is valid
		if (!header.isValid(outgoingBuffer)) return true;	
		int zmqFlags = isBlocking? 0 : ZMQ.NOBLOCK;
		
		// check event bounds
		int segmentCount = header.getSegmentCount();
		int lastSegmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentCount - 1);
		int lastSegmentOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		int lastSegmentLength = EventHeader.getSegmentLength(lastSegmentMetaData);
		int requiredLength = lastSegmentOffset + lastSegmentLength;
		if (requiredLength > outgoingBuffer.length) {
			header.setSentTime(outgoingBuffer, -1);
			throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
					outgoingBuffer.length, requiredLength));
		}
		
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
			if (!doSend(_socket, outgoingBuffer, offset, length, flags)) {
				header.setSentTime(outgoingBuffer, -1);
				header.setNextSegmentToSend(outgoingBuffer, segmentIndex);
				header.setIsPartiallySent(outgoingBuffer, true);
				return false;
			}
		}
		header.setSentTime(outgoingBuffer, _clock.currentMillis());
		return true;
	}
	
	private boolean doSend(ZMQ.Socket socket, byte[] outgoingBuffer, int offset, int length, int sendFlags) {
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
			
	@Override
	public boolean recv(byte[] eventBuffer, IncomingEventHeader header,
			boolean isBlocking) {
		int cursor = header.getEventOffset();
		int expectedSegmentCount = header.getSegmentCount();
		int segmentIndex = 0;
		boolean isValid = true;
		long recvTime = -1;
		
		do {
			if (segmentIndex > expectedSegmentCount || cursor >= eventBuffer.length) {
				isValid = false;
			}
			
			if (isValid) {
				int recvdAmount = doRecv(_socket, eventBuffer, cursor, eventBuffer.length - cursor, isBlocking);
				if (segmentIndex == 0 && recvdAmount == 0) {
					// no message ready
					return false;
				} else if (recvdAmount == -1) {
					isValid = false;
				} else {
					if (recvTime == -1) recvTime = _clock.currentMillis();
					header.setSegmentMetaData(eventBuffer, segmentIndex, cursor, recvdAmount);					
					cursor += recvdAmount;
					segmentIndex++;
				}
			} else {
				// absorb remaining segments
				_socket.recv();
			}
		} while (_socket.hasReceiveMore());
		
		header.setIsValid(eventBuffer, isValid);
		header.setSocketId(eventBuffer, _socketId);
		header.setRecvTime(eventBuffer, recvTime);
		return true;
	}
	
	private int doRecv(ZMQ.Socket socket, byte[] eventBuffer, int offset, int length, boolean isBlocking) {
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
			// if we get an exception, there was an error besides EAGAIN (jzmq handles this for us)
			// check that the socket hasn't just been closed
			if (eZmq.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
				throw MessengerClosedException.INSTANCE;
			} else {
				return -1;
			}
		}
	}

	@Override
	public int[] getEndpointIds() {
		return new int[] { _socketId };
	}
	
	public int getSocketId() {
		return _socketId;
	}
	
	public ZMQ.Socket getSocket() {
		return _socket;
	}

	@Override
	public boolean hasPendingEvents() {
		return (_socket.getEvents() & ZMQ.Poller.POLLIN) != 0;
	}

}
