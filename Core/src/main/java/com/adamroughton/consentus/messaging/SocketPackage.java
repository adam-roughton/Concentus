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

import org.zeromq.ZMQ;

import com.adamroughton.consentus.messaging.MessageFrameBufferMapping.NamedOffset;

public final class SocketPackage {
	
	private final ZMQ.Socket _socket;
	private final MessageFrameBufferMapping _messagePartPolicy;
	private final int _socketId;
	
	public static SocketPackage create(ZMQ.Socket socket) {
		return new SocketPackage(socket, new MessageFrameBufferMapping(0), 0);
	}
	
	private SocketPackage(
			final ZMQ.Socket socket,
			final MessageFrameBufferMapping messagePartPolicy,
			final int socketId) {
		_socket = socket;
		_messagePartPolicy = messagePartPolicy;
		_socketId = socketId;
	}
	
	/**
	 * The offsets to apply to the incoming event buffer when receiving the event parts. 
	 * All offsets are relative to any reserved byte offset.
	 * @param firstOffset
	 * @param subsequentOffsets
	 * @return
	 */
	public SocketPackage setMessageOffsets(final int firstOffset, int... subsequentOffsets) {
		int[] offsets = new int[subsequentOffsets.length + 1];
		offsets[0] = firstOffset;
		System.arraycopy(subsequentOffsets, 0, offsets, 1, subsequentOffsets.length);
		MessageFrameBufferMapping messagePartPolicy = new MessageFrameBufferMapping(offsets);
		return new SocketPackage(_socket, messagePartPolicy, _socketId);
	}
	
	/**
	 * The offsets to apply to the incoming event buffer when receiving the event parts. 
	 * All offsets are relative to any reserved byte offset.
	 * @param firstOffset
	 * @param subsequentOffsets
	 * @return
	 */
	public SocketPackage setMessageOffsets(final NamedOffset firstOffset, NamedOffset... subsequentOffsets) {
		NamedOffset[] offsets = new NamedOffset[subsequentOffsets.length + 1];
		offsets[0] = firstOffset;
		System.arraycopy(subsequentOffsets, 0, offsets, 1, subsequentOffsets.length);
		MessageFrameBufferMapping messagePartPolicy = new MessageFrameBufferMapping(offsets);
		return new SocketPackage(_socket, messagePartPolicy, _socketId);
	}
	
	public SocketPackage setMessageOffsets(final MessageFrameBufferMapping offsetPolicy) {
		return new SocketPackage(_socket, offsetPolicy, _socketId);
	}
	
	/**
	 * An optional identifier that will be written in to an attached
	 * buffer as part of a header.
	 * 
	 * @param socketId
	 * @return
	 */
	public SocketPackage setSocketId(final int socketId) {
		return new SocketPackage(_socket, _messagePartPolicy, socketId);
	}
	
	public MessageFrameBufferMapping getMessageFrameBufferMapping() {
		return new MessageFrameBufferMapping(_messagePartPolicy);
	}
	
	/**
	 * An optional identifier that will be written in to an attached
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
}
