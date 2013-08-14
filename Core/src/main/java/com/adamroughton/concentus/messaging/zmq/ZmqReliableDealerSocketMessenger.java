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

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.esotericsoftware.minlog.Log;

public final class ZmqReliableDealerSocketMessenger implements ZmqSocketMessenger {

	private final int _socketId;
	private final String _name;
	private final ZMQ.Socket _socket;
	private final Clock _clock;
	
	private long _lastRecvReliableSeq = -1;
	private long _nack = -1;
	private final byte[] _headerBytes = new byte[8];
	
	public ZmqReliableDealerSocketMessenger(int socketId, String name, ZMQ.Socket socket, Clock clock) {
		_socketId = socketId;
		_name = Objects.requireNonNull(name);
		_socket = Objects.requireNonNull(socket);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public boolean send(byte[] outgoingBuffer, 
			OutgoingEventHeader header,
			boolean isBlocking) {
		// only send if the event is valid
		if (!header.isValid(outgoingBuffer)) return true;	
		
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
		
		int startSegmentIndex;
		if (header.isPartiallySent(outgoingBuffer)) {
			startSegmentIndex = header.getNextSegmentToSend(outgoingBuffer);
		} else {
			startSegmentIndex = 0;
		}
		
		if (startSegmentIndex == 0) {
			if (!sendHeader(isBlocking))
				return false;
		}
		
		int lastSegmentIndex = segmentCount - 1;
		int currentSegmentIndex = ZmqSocketOperations.sendSegments(_socket, outgoingBuffer, header, startSegmentIndex, lastSegmentIndex, isBlocking);
		if (currentSegmentIndex != lastSegmentIndex) {
			header.setSentTime(outgoingBuffer, -1);
			header.setNextSegmentToSend(outgoingBuffer, currentSegmentIndex);
			header.setIsPartiallySent(outgoingBuffer, true);
			return false;
		}
		header.setSentTime(outgoingBuffer, _clock.currentMillis());
		return true;
	}
		
	public boolean sendHeader(boolean isBlocking) {
		MessageBytesUtil.writeLong(_headerBytes, 0, _nack);
		return ZmqSocketOperations.doSend(_socket, _headerBytes, 0, _headerBytes.length, (isBlocking? 0 : ZMQ.NOBLOCK) | ZMQ.SNDMORE);
	}
	
	@Override
	public boolean recv(byte[] eventBuffer, IncomingEventHeader header,
			boolean isBlocking) {
		int cursor = header.getEventOffset();
		int expectedSegmentCount = header.getSegmentCount() + 1;
		int eventSegmentIndex = 0;
		int segmentIndex = 0;
		boolean isValid = true;
		long recvTime = -1;
		
		do {
			if (segmentIndex > expectedSegmentCount || cursor >= eventBuffer.length) {
				isValid = false;
			}
			
			if (isValid) {
				if (segmentIndex == 0) {
					int retVal = processSeqFrame(isBlocking, eventBuffer, header);
					if (retVal == 0) {
						return false;
					} else {
						isValid = retVal > 0;
					}
					segmentIndex++;
				} else {
					int recvdAmount = ZmqSocketOperations.doRecv(_socket, eventBuffer, cursor, eventBuffer.length - cursor, isBlocking);
					if (segmentIndex == 0 && recvdAmount == 0) {
						// no message ready
						return false;
					} else if (recvdAmount == -1) {
						isValid = false;
					} else {
						if (recvTime == -1) recvTime = _clock.currentMillis();
						header.setSegmentMetaData(eventBuffer, eventSegmentIndex, cursor, recvdAmount);					
						cursor += recvdAmount;
						segmentIndex++;
						eventSegmentIndex++;
					}
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
	
	public int processSeqFrame(boolean isBlocking, byte[] incomingBuffer, IncomingEventHeader eventHeader) {	
		// get seq header
		int headerSize = ZmqSocketOperations.doRecv(_socket, _headerBytes, 0, _headerBytes.length, isBlocking);
		if (headerSize == 0) {
			return 0;
		} else if (headerSize == -1) {
			return -1;
		} else if (headerSize != _headerBytes.length) {
			Log.warn(String.format("Expected to find a " +
					"reliable seq message frame of length 8 bytes, " +
					"but instead found %d bytes.", headerSize));
			return -1;
		} else {
			long seq = MessageBytesUtil.readLong(_headerBytes, 0);
			
			if (seq != -1) {
				if (seq == Long.MAX_VALUE) {
					eventHeader.setConnectionInvalid(incomingBuffer, true);
				} else if (seq <= _lastRecvReliableSeq) {
					// drop duplicate message
					return -1;
				} else if (seq == _nack) {
					_nack = -1;
					_lastRecvReliableSeq = seq;
				} else if (seq == _lastRecvReliableSeq + 1) {
					_lastRecvReliableSeq = seq;
				} else {
					_nack = _lastRecvReliableSeq + 1;
					//send nack
					MessageBytesUtil.writeLong(_headerBytes, 0, _nack);
					ZmqSocketOperations.doSend(_socket, _headerBytes, 0, _headerBytes.length, (isBlocking? 0 : ZMQ.NOBLOCK));
					return -1; // drop out of order message: easy way of avoiding duplicates
				}
			}
			return headerSize;
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
	public String name() {
		return _name;
	}

}
