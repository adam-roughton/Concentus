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

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.esotericsoftware.minlog.Log;

public final class ZmqStandardSocketMessenger implements ZmqSocketMessenger {

	private final int _socketId;
	private final String _name;
	private final ZMQ.Socket _socket;
	private final int _socketType;
	
	private final byte[] _headerBytes;
	
	// maximum 0MQ identity is 255 bytes, so ensure enough space just in case
	private final byte[] _socketIdTmpBuffer = new byte[255];
	
	public ZmqStandardSocketMessenger(int socketId, String name, ZMQ.Socket socket) {
		_socketId = socketId;
		_name = Objects.requireNonNull(name);
		_socket = Objects.requireNonNull(socket);
		
		_socketType = socket.getType();
		if (_socketType == ZMQ.PUB || _socketType == ZMQ.SUB) {
			_headerBytes = new byte[ResizingBuffer.INT_SIZE * 2];
		} else {
			_headerBytes = new byte[ResizingBuffer.INT_SIZE];
		}
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
			throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
					bufferContentSize, requiredLength));
		}
		
		int startSegmentIndex;
		if (header.isPartiallySent(outgoingBuffer)) {
			startSegmentIndex = header.getNextSegmentToSend(outgoingBuffer);
		} else {
			startSegmentIndex = 0;
		}
		
		int segmentIndex;
		if (_socketType == ZMQ.ROUTER) {
			segmentIndex = sendRouter(bufferContentSize, segmentCount, startSegmentIndex, outgoingBuffer, header, isBlocking);
		} else {
			segmentIndex = sendStandard(bufferContentSize, segmentCount, startSegmentIndex, outgoingBuffer, header, isBlocking);
		}
		
		if (segmentIndex != segmentCount - 1) {
			header.setNextSegmentToSend(outgoingBuffer, segmentIndex);
			header.setIsPartiallySent(outgoingBuffer, true);
			return false;
		}
		return true;
	}
		
	private int sendRouter(
			int bytesInBuffer,
			int segmentCount,
			int startSegmentIndex,
			ArrayBackedResizingBuffer outgoingBuffer, 
			OutgoingEventHeader header,
			boolean isBlocking) {
		boolean isSending = true;
		int segmentIndex = startSegmentIndex;
		do {
			if (segmentIndex == 0) {
				// send identity bytes (for ROUTER socket)
				if (ZmqSocketOperations.sendSegments(_socket, outgoingBuffer, header, 0, 0, isBlocking) == 0) {
					segmentIndex = 1;	
				} else {
					isSending = false;
				}
			} else if (segmentIndex == 1) {
				// send header
				// get the msg size leaving out the identity frame which is not sent
				int firstContentFrameMetaData = header.getSegmentMetaData(outgoingBuffer, 1);
				int contentOffset = EventHeader.getSegmentOffset(firstContentFrameMetaData);
				if (sendHeader(bytesInBuffer - contentOffset, outgoingBuffer, header, isBlocking)) {	
					segmentIndex = 2;
				} else {
					isSending = false;
				}
			} else {
				segmentIndex = ZmqSocketOperations.sendSegments(_socket, outgoingBuffer, header, segmentIndex - 1, segmentCount - 1, isBlocking);
				isSending = false;
			}
		} while (isSending);
		
		return segmentIndex;
	}
	
	private int sendStandard(
			int bytesInBuffer,
			int segmentCount,
			int startSegmentIndex,
			ArrayBackedResizingBuffer outgoingBuffer, 
			OutgoingEventHeader header,
			boolean isBlocking) {
		boolean isSending = true;
		int segmentIndex = startSegmentIndex;
		do {
			if (segmentIndex == 0) {
				isSending = sendHeader(bytesInBuffer - header.getEventOffset(), outgoingBuffer, header, isBlocking);
				if (isSending) {
					segmentIndex = _socketType == ZMQ.PUB? 2 : 1; // the first frame is the subId which we have put in the header
				}
			} else {
				int lastSegmentIndex = segmentCount - 1;
				segmentIndex = ZmqSocketOperations.sendSegments(_socket, outgoingBuffer, header, segmentIndex - 1, lastSegmentIndex, isBlocking);
				isSending = false;
			}
		} while (isSending);
		
		return segmentIndex;
	}
	
	public boolean sendHeader(int msgSize, ArrayBackedResizingBuffer eventBuffer, 
			OutgoingEventHeader header, boolean isBlocking) {
		// bit hacky for now, but if the socket is a pub socket put the first Int bytes of the 
		// event content (event type) first for subscription filtering
		int cursor = 0;
		if (_socketType == ZMQ.PUB) {
			int eventTypeSegmentMetaData = header.getSegmentMetaData(eventBuffer, 0);
			int eventTypeOffset = EventHeader.getSegmentOffset(eventTypeSegmentMetaData);
			int eventTypeId = eventBuffer.readInt(eventTypeOffset);
			
			BytesUtil.writeInt(_headerBytes, cursor, eventTypeId);
			cursor += ResizingBuffer.INT_SIZE;
		}
		
		BytesUtil.writeInt(_headerBytes, cursor, msgSize);
		return ZmqSocketOperations.doSend(_socket, _headerBytes, 0, _headerBytes.length, (isBlocking? 0 : ZMQ.NOBLOCK) | ZMQ.SNDMORE);
	}
	
	@Override
	public boolean recv(ArrayBackedResizingBuffer eventBuffer, 
			IncomingEventHeader header,
			boolean isBlocking) {
		/*
		 * Prepare the buffer to receive new event bytes. We do this
		 * for both performance reasons (want to use primary array
		 * as much as possible) and for content size reporting.
		 */
		eventBuffer.reset();
		
		int cursor = header.getEventOffset();
		int expectedSegmentCount = header.getSegmentCount() + 1;
		int eventSegmentIndex = 0;
		int segmentIndex = 0;
		boolean isValid = true;
		int lastCursor = -1;
		
		do {
			if (segmentIndex > expectedSegmentCount) {
				isValid = false;
			}
			
			if (isValid) {
				if ((_socketType != ZMQ.ROUTER && segmentIndex == 0) || (_socketType == ZMQ.ROUTER && segmentIndex == 1)) {
					int retVal = processHeaderFrame(eventBuffer, cursor, isBlocking);
					if (retVal == 0) {
						return false;
					} else {
						isValid = retVal > 0;
					}
					segmentIndex++;
				} else {
					int recvdAmount;
					if (segmentIndex == 0 && _socketType == ZMQ.ROUTER) {
						recvdAmount = ZmqSocketOperations.doRecv(_socket, _socketIdTmpBuffer, 0, _socketIdTmpBuffer.length, isBlocking);
						eventBuffer.copyFrom(_socketIdTmpBuffer, 0, cursor, recvdAmount);
					} else {
						byte[] eventBufferByteArray = eventBuffer.getBuffer();
						recvdAmount = ZmqSocketOperations.doRecv(_socket, eventBufferByteArray, cursor, eventBufferByteArray.length - cursor, isBlocking);
					}
					
					if (segmentIndex == 0 && recvdAmount == 0) {
						// no message ready
						return false;
					} else if (recvdAmount == -1) {
						isValid = false;
					} else {					
						header.setSegmentMetaData(eventBuffer, eventSegmentIndex, cursor, recvdAmount);	
						lastCursor = cursor;
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
		
		boolean isMessagingEvent = false;
		if (isValid) {
			int eventId = eventBuffer.readInt(lastCursor);
			if (eventId < 0) isMessagingEvent = true;
		}
		header.setIsMessagingEvent(eventBuffer, isMessagingEvent);
		header.setIsValid(eventBuffer, isValid);
		header.setSocketId(eventBuffer, _socketId);
		return true;
	}
	
	public int processHeaderFrame(ArrayBackedResizingBuffer incomingBuffer, int cursor, boolean isBlocking) {	
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
			int headerCursor = 0;
			if (_socketType == ZMQ.SUB) {
				// if sub socket, ignore the event type ID
				// bytes as they have already served their purpose
				// for subscription filtering
				headerCursor += ResizingBuffer.INT_SIZE;
			}
			
			int msgSize = BytesUtil.readInt(_headerBytes, headerCursor);
			incomingBuffer.preallocate(cursor, msgSize);
			
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
