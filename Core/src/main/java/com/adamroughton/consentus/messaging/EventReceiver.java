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
package com.adamroughton.consentus.messaging;

import java.util.Objects;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class EventReceiver {

	private final EventProcessingHeader _header;
	private final int _msgOffset;
	private final int _baseZmqFlag;
	
	public EventReceiver(final EventProcessingHeader processingHeader, 
			final boolean isNoBlock) {
		_header = Objects.requireNonNull(processingHeader);
		_msgOffset = _header.getEventOffset();
		
		if (isNoBlock) {
			_baseZmqFlag = ZMQ.NOBLOCK;
		} else {
			_baseZmqFlag = 0;
		}
	}
	
	/**
	 * Receives an event on the given socket, filling the given
	 * event buffer as per the given message parts policy.
	 * @param socketPackage the socket (plus additional settings) to receive on
	 * @param eventBuffer the buffer to place the event in
	 * @return whether an event was placed in the buffer
	 */
	public boolean recv(final SocketPackage socketPackage, 
			final byte[] eventBuffer) {
		return recv(socketPackage.getSocket(), 
				socketPackage.getMessageFrameBufferMapping(), 
				socketPackage.getSocketId(), 
				eventBuffer);
	}
	
	/**
	 * Receives an event on the given socket, filling the given
	 * event buffer as per the given message parts policy.
	 * @param socket the socket to receive on
	 * @param mapping the message frame mapping to apply to incoming messages
	 * @param socketId the socket ID to write into the header of incoming messages
	 * @param eventBuffer the buffer to place the event in
	 * @return whether an event was placed in the buffer
	 */
	public boolean recv(final ZMQ.Socket socket,
			MessageFrameBufferMapping mapping,
			int socketId,
			final byte[] eventBuffer) {		
		int msgOffsetIndex = 0;
		int expMsgParts = mapping.partCount();
		int offset;
		boolean isValid = true;
		
		// get the first offset, or use a default if the policy is not valid
		if (mapping.getMinReqBufferSize() > eventBuffer.length - _msgOffset) {
			offset = _msgOffset;
			isValid = false;
		} else if (expMsgParts == 0) {
			offset = _msgOffset;
		} else {
			offset = _msgOffset + mapping.getOffset(0);
		}
		
		// check if we have any messages waiting
		int recvdAmount = doRecv(socket, eventBuffer, offset, eventBuffer.length - offset, _baseZmqFlag);
		if (recvdAmount == 0) {
			// no message ready
			return false;
		} else {		
			if (recvdAmount == -1) {
				isValid = false;
			}
			
			while(isValid && socket.hasReceiveMore() && ++msgOffsetIndex < expMsgParts) {
				offset = _msgOffset + mapping.getOffset(msgOffsetIndex);
				recvdAmount = doRecv(socket, eventBuffer, offset, eventBuffer.length - offset, _baseZmqFlag);
				if (recvdAmount == -1) {
					isValid = false;
				}
			}
			
			if (msgOffsetIndex < expMsgParts - 1) {
				isValid = false;
			}
			
			// capture any additional message parts
			while (socket.hasReceiveMore()) {
				socket.recv(0);
			}
			
			_header.setIsValid(isValid, eventBuffer);
			_header.setSocketId(socketId, eventBuffer);
			return true;
		}
	}
	
	private int doRecv(ZMQ.Socket socket, byte[] eventBuffer, int offset, int length, int zmqFlags) {
		try {
			int recvAmount = socket.recv(eventBuffer, offset, eventBuffer.length - offset, _baseZmqFlag);
			return recvAmount == -1? 0 : recvAmount;
		} catch (ZMQException eZmq) {
			// if we get an exception, there was an error besides EAGAIN
			return -1;
		}
	}
	
}
