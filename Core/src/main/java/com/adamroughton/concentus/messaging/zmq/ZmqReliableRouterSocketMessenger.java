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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.zeromq.ZMQ;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.esotericsoftware.minlog.Log;

public final class ZmqReliableRouterSocketMessenger implements ZmqSocketMessenger {

	private final int _socketId;
	private final String _name;
	private final ZMQ.Socket _socket;
	private final Clock _clock;
	
	private final Map<SocketIdentity, ReliableSeqInfo> _reliableSeqLookup = new HashMap<>();
	private final StructuredSlidingWindowMap<CachedMessage> _reliableMsgBuffer;
	private final long _tryAgainMillis;	
	private final byte[] _headerBytes = new byte[ResizingBuffer.LONG_SIZE + ResizingBuffer.INT_SIZE];
	
	public ZmqReliableRouterSocketMessenger(int socketId, String name, ZMQ.Socket socket, Clock clock, 
			long tryAgainMillis, int reliableBufferSize, final int msgLength) {
		if (socket.getType() != ZMQ.ROUTER)
			throw new IllegalArgumentException("Only ROUTER sockets supported by this messenger");
		_socketId = socketId;
		_name = Objects.requireNonNull(name);
		_socket = Objects.requireNonNull(socket);
		_clock = Objects.requireNonNull(clock);
		_tryAgainMillis = tryAgainMillis;
		
		_reliableMsgBuffer = new StructuredSlidingWindowMap<>(reliableBufferSize, CachedMessage.class, 
				new ComponentFactory<CachedMessage>() {

					@Override
					public CachedMessage newInstance(Object[] initArgs) {
						return new CachedMessage(msgLength);
					}
				}, new InitialiseDelegate<CachedMessage>() {

					@Override
					public void initialise(CachedMessage content) {
						content.reset();
					}
				});
	}
	
	@Override
	public boolean send(ArrayBackedResizingBuffer outgoingBuffer, 
			OutgoingEventHeader header,
			boolean isBlocking) {
		// only send if the event is valid
		if (!header.isValid(outgoingBuffer)) return true;	
		
		// check event bounds
		int segmentCount = header.getSegmentCount();
		if (segmentCount < 2)
			throw new RuntimeException("The minimum number of frames for a ROUTER socket is 2");
		
		int bytesInBuffer = outgoingBuffer.getContentSize();
		int lastSegmentMetaData = header.getSegmentMetaData(outgoingBuffer, segmentCount - 1);
		int lastSegmentOffset = EventHeader.getSegmentOffset(lastSegmentMetaData);
		int lastSegmentLength = EventHeader.getSegmentLength(lastSegmentMetaData);
		int requiredLength = lastSegmentOffset + lastSegmentLength;
		if (requiredLength > bytesInBuffer) {
			header.setSentTime(outgoingBuffer, -1);
			throw new RuntimeException(String.format("The buffer length is less than the content length (%d < %d)", 
					bytesInBuffer, requiredLength));
		}
		
		int segmentIndex;
		if (header.isPartiallySent(outgoingBuffer)) {
			segmentIndex = header.getNextSegmentToSend(outgoingBuffer);
		} else {
			segmentIndex = 0;
		}
		
		segmentIndex = sendWithReliableFrame(false, 0, bytesInBuffer, segmentCount, segmentIndex, outgoingBuffer, header, isBlocking);
		
		if (segmentIndex == 0) {
			return false;
		} else if (segmentIndex < segmentCount - 1) {
			header.setSentTime(outgoingBuffer, -1);
			header.setNextSegmentToSend(outgoingBuffer, segmentIndex);
			header.setIsPartiallySent(outgoingBuffer, true);
			return false;
		} else {
			header.setSentTime(outgoingBuffer, _clock.currentMillis());
			return true;
		}
	}
	
	private int sendWithReliableFrame(
			boolean isResend,
			long seq, 
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
				if (!isResend) {
					seq = assignSeq(getIdentity(outgoingBuffer, header), outgoingBuffer, header);
				}
				// get the msg size leaving out the identity frame which is not sent
				int firstContentFrameMetaData = header.getSegmentMetaData(outgoingBuffer, 1);
				int contentOffset = EventHeader.getSegmentOffset(firstContentFrameMetaData);
				if (sendHeader(seq, bytesInBuffer - contentOffset, outgoingBuffer, header, isBlocking)) {	
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
	
	private static SocketIdentity getIdentity(ArrayBackedResizingBuffer messageBuffer, EventHeader header) {
		int segmentMetaData = header.getSegmentMetaData(messageBuffer, 0);
		int offset = EventHeader.getSegmentOffset(segmentMetaData);
		int length = EventHeader.getSegmentLength(segmentMetaData);
		return new SocketIdentity(messageBuffer.getBuffer(), offset, length);
	}
	
	private long assignSeq(SocketIdentity identity, ArrayBackedResizingBuffer outgoingBuffer, OutgoingEventHeader eventHeader) {
		long seqToWrite;
		
		ReliableSeqInfo reliableSeqInfo;
		if (_reliableSeqLookup.containsKey(identity)) {
			reliableSeqInfo = _reliableSeqLookup.get(identity);
		} else {
			reliableSeqInfo = new ReliableSeqInfo();
			_reliableSeqLookup.put(identity.copyWithNewArray(), reliableSeqInfo);
		}
		if (reliableSeqInfo.reliableSeq != Long.MAX_VALUE && eventHeader.isReliable(outgoingBuffer)) {			
			reliableSeqInfo.reliableSeq++;
				
			_reliableMsgBuffer.advanceTo(reliableSeqInfo.reliableSeq);
			CachedMessage msgCache = _reliableMsgBuffer.get(reliableSeqInfo.reliableSeq);
			outgoingBuffer.copyTo(msgCache.messageBuffer);
			
			// clear any processing flags from the buffered event
			eventHeader.setIsPartiallySent(msgCache.messageBuffer, false); 
			msgCache.eventHeader = eventHeader;
			
			seqToWrite = reliableSeqInfo.reliableSeq;
		} else {
			// if the reliable seq is Long.MAX_VALUE, the connection is no longer
			// able to support reliable messages due to dropped messages
			// we keep sending this signal to the client 
			if (reliableSeqInfo.reliableSeq == Long.MAX_VALUE) {
				seqToWrite = Long.MAX_VALUE;
			} else {
				seqToWrite = -1;
			}
		}
		return seqToWrite;
	}
	
	private boolean sendHeader(long seq, int msgSize, ArrayBackedResizingBuffer outgoingBuffer, OutgoingEventHeader eventHeader, boolean isBlocking) {
		BytesUtil.writeLong(_headerBytes, 0, seq);
		BytesUtil.writeInt(_headerBytes, ResizingBuffer.LONG_SIZE, msgSize);
		return ZmqSocketOperations.doSend(_socket, _headerBytes, 0, _headerBytes.length, (isBlocking? 0 : ZMQ.NOBLOCK) | ZMQ.SNDMORE);
	}
			
	@Override
	public boolean recv(ArrayBackedResizingBuffer eventBuffer, IncomingEventHeader header,
			boolean isBlocking) {
		int cursor = header.getEventOffset();
		int expectedSegmentCount = header.getSegmentCount() + 1;
		int eventSegmentIndex = 0;
		int segmentIndex = 0;
		boolean isValid = true;
		long recvTime = -1;
		
		boolean isNackMsg = true;
		do {
			if (segmentIndex > expectedSegmentCount) {
				isValid = false;
			}
			
			if (isValid) {
				if (segmentIndex == 1) {
					if (processHeaderFrame(eventBuffer, header, cursor, getIdentity(eventBuffer, header), isBlocking) == -1) {
						isValid = false;	
					}
					segmentIndex++;
				} else {
					byte[] msgBufferByteArray;
					if (segmentIndex == 0) {
						// maximum 0MQ identity is 255 bytes, so ensure enough space just in case
						msgBufferByteArray = eventBuffer.allocateForWriting(255);
					} else {
						msgBufferByteArray = eventBuffer.getBuffer();
					}
					int recvdAmount = ZmqSocketOperations.doRecv(_socket, msgBufferByteArray, cursor, msgBufferByteArray.length, isBlocking);
					if (segmentIndex == 0 && recvdAmount == 0) {
						// no message ready
						return false;
					} else if (recvdAmount == -1) {
						isValid = false;
					} else {
						// NACK msgs have 0 content
						if (eventSegmentIndex > 0) isNackMsg = false;
						
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
		
		header.setIsValid(eventBuffer, isValid && !isNackMsg);
		header.setSocketId(eventBuffer, _socketId);
		header.setRecvTime(eventBuffer, recvTime);
		return true;
	}
	
	public int processHeaderFrame(ArrayBackedResizingBuffer incomingBuffer, IncomingEventHeader eventHeader, int contentStartOffset, SocketIdentity identity, boolean isBlocking) {	
		ReliableSeqInfo reliableSeqInfo;
		if (_reliableSeqLookup.containsKey(identity)) {
			reliableSeqInfo = _reliableSeqLookup.get(identity);
		} else {
			reliableSeqInfo = new ReliableSeqInfo();
			_reliableSeqLookup.put(identity.copyWithNewArray(), reliableSeqInfo);
		}
		
		// get NACK header
		int headerSize = ZmqSocketOperations.doRecv(_socket, _headerBytes, 0, _headerBytes.length, isBlocking);
		if (headerSize <= 0) {
			return -1;
		} else if (headerSize != _headerBytes.length) {
			Log.warn(String.format("Expected to find a " +
					"reliable seq message frame of length 8 bytes, " +
					"but instead found %d bytes.", headerSize));
			return -1;
		} else {
			long nackSeq = BytesUtil.readLong(_headerBytes, 0);
			int msgSize = BytesUtil.readInt(_headerBytes, ResizingBuffer.LONG_SIZE);
			incomingBuffer.preallocate(contentStartOffset, msgSize + contentStartOffset);
			
			if (nackSeq == -1) {
				reliableSeqInfo.lastRecvNack = -1;	
				reliableSeqInfo.millisSinceNackResponse = -1;
			} else {
				boolean resend;
				if (reliableSeqInfo.lastRecvNack != nackSeq) {
					reliableSeqInfo.lastRecvNack = nackSeq;
					reliableSeqInfo.millisSinceNackResponse = _clock.currentMillis();
					resend = true;
				} else if (_clock.currentMillis() > reliableSeqInfo.millisSinceNackResponse + _tryAgainMillis) {
					resend = true;
				} else {
					resend = false;
				}
				if (resend) {
					if (_reliableMsgBuffer.containsIndex(nackSeq)) {
						// send replays
						for (long seq = nackSeq; seq <= _reliableMsgBuffer.getHeadIndex(); seq++) {
							CachedMessage cachedMsg = _reliableMsgBuffer.get(seq);
							int segmentCount = cachedMsg.eventHeader.getSegmentCount();
							int cachedBytesInBuffer = cachedMsg.messageBuffer.getContentSize();
							
							if (sendWithReliableFrame(true, seq, cachedBytesInBuffer, segmentCount, 0, cachedMsg.messageBuffer, cachedMsg.eventHeader, false) != segmentCount -1) {
								throw new RuntimeException("Sending on ROUTER socket not expected to fail");
							}
						}
					} else {
						// send seq frame indicating an inability to fulfill NACK
						BytesUtil.writeLong(_headerBytes, 0, Long.MAX_VALUE);
						boolean sendSucceeded = true;
						sendSucceeded &= ZmqSocketOperations.doSend(_socket, identity.buffer, identity.offset, identity.length, (isBlocking? 0 : ZMQ.NOBLOCK) | ZMQ.SNDMORE);
						if (sendSucceeded) {
							sendSucceeded &= ZmqSocketOperations.doSend(_socket, _headerBytes, 0, _headerBytes.length, 0);
						}
						if (!sendSucceeded){
							throw new RuntimeException("Sending on ROUTER socket not expected to fail");
						}
					}
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

	private static class CachedMessage {
		public final ArrayBackedResizingBuffer messageBuffer;
		public OutgoingEventHeader eventHeader = null;
		
		public CachedMessage(int bufferSize) {
			messageBuffer = new ArrayBackedResizingBuffer(bufferSize);
		}
		
		public void reset() {
			messageBuffer.reset();
			eventHeader = null;
		}
	}
	
	private static class ReliableSeqInfo {
		public long reliableSeq = -1;
		public long lastRecvNack = -1;
		/* allow us to deal with re-sending events that have been 
		 * dropped again after responding to a previous NACK */
		public long millisSinceNackResponse = -1; 
	}
	
}
