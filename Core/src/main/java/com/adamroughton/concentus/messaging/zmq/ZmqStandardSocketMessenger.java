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
import com.adamroughton.concentus.messaging.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.esotericsoftware.minlog.Log;

public final class ZmqStandardSocketMessenger implements ZmqSocketMessenger {

	private final int _socketId;
	private final String _name;
	private final ZMQ.Socket _socket;
	private final Clock _clock;
	
	private final byte[] _headerBytes = new byte[ResizingBuffer.INT_SIZE];
	
	public ZmqStandardSocketMessenger(int socketId, String name, ZMQ.Socket socket, Clock clock) {
		_socketId = socketId;
		_name = Objects.requireNonNull(name);
		_socket = Objects.requireNonNull(socket);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public boolean send(ArrayBackedResizingBuffer outgoingBuffer, 
			OutgoingEventHeader header,
			boolean isBlocking) {
		// only send if the event is valid
		if (!header.isValid(outgoingBuffer)) return true;	
		
		// check event bounds
		int segmentCount = header.getSegmentCount();
		int bufferContentSize = outgoingBuffer.getContentSize();
		int lastSegmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentCount - 1);
		int lastSegmentOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		int lastSegmentLength = EventHeader.getSegmentLength(lastSegmentMetaData);
		int requiredLength = lastSegmentOffset + lastSegmentLength;
		if (requiredLength > bufferContentSize) {
			header.setSentTime(outgoingBuffer, -1);
			throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
					bufferContentSize, requiredLength));
		}
		
		int startSegmentIndex;
		if (header.isPartiallySent(outgoingBuffer)) {
			startSegmentIndex = header.getNextSegmentToSend(outgoingBuffer);
		} else {
			startSegmentIndex = 0;
		}
		
		if (startSegmentIndex == 0) {
			if (!sendHeader(bufferContentSize - header.getEventOffset(), isBlocking))
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
		
	public boolean sendHeader(int msgSize, boolean isBlocking) {
		MessageBytesUtil.writeInt(_headerBytes, 0, msgSize);
		return ZmqSocketOperations.doSend(_socket, _headerBytes, 0, _headerBytes.length, (isBlocking? 0 : ZMQ.NOBLOCK) | ZMQ.SNDMORE);
	}
	
	@Override
	public boolean recv(ArrayBackedResizingBuffer eventBuffer, 
			IncomingEventHeader header,
			boolean isBlocking) {
		int cursor = header.getEventOffset();
		int expectedSegmentCount = header.getSegmentCount() + 1;
		int eventSegmentIndex = 0;
		int segmentIndex = 0;
		boolean isValid = true;
		long recvTime = -1;
		
		do {
			if (segmentIndex > expectedSegmentCount) {
				isValid = false;
			}
			
			if (isValid) {
				if (segmentIndex == 0) {
					int retVal = processHeaderFrame(eventBuffer, header, isBlocking);
					if (retVal == 0) {
						return false;
					} else {
						isValid = retVal > 0;
					}
					segmentIndex++;
				} else {
					byte[] eventBufferByteArray = eventBuffer.getBuffer();
					int recvdAmount = ZmqSocketOperations.doRecv(_socket, eventBufferByteArray, cursor, eventBufferByteArray.length - cursor, isBlocking);
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
	
	public int processHeaderFrame(ArrayBackedResizingBuffer incomingBuffer, IncomingEventHeader eventHeader, boolean isBlocking) {	
		int headerSize = ZmqSocketOperations.doRecv(_socket, _headerBytes, 0, _headerBytes.length, isBlocking);
		if (headerSize == 0) {
			return 0;
		} else if (headerSize == -1) {
			return -1;
		} else if (headerSize != _headerBytes.length) {
			Log.warn(String.format("Expected to find a " +
					"header frame of length %d bytes, " +
					"but instead found %d bytes.", _headerBytes.length, headerSize));
			return -1;
		} else {
			int msgSize = MessageBytesUtil.readInt(_headerBytes, 0);
			incomingBuffer.allocateForWriting(msgSize + eventHeader.getEventOffset());
			
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
