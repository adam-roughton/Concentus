package com.adamroughton.concentus.messaging.zmq;

import java.util.Objects;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;

public class ZmqStandardSocketMessenger implements ZmqSocketMessenger {
	private final int _socketId;
	private final String _name;
	private final ZMQ.Socket _socket;
	private final Clock _clock;
	
	public ZmqStandardSocketMessenger(int socketId, String name, ZMQ.Socket socket, Clock clock) {
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
		
		int lastSegmentIndex = segmentCount - 1;
		int currentSegmentIndex = ZmqSocketOperations.sendSegments(_socket, 
				outgoingBuffer, header, startSegmentIndex, lastSegmentIndex, isBlocking);
		if (currentSegmentIndex != lastSegmentIndex) {
			header.setSentTime(outgoingBuffer, -1);
			header.setNextSegmentToSend(outgoingBuffer, currentSegmentIndex);
			header.setIsPartiallySent(outgoingBuffer, true);
			return false;
		}
		header.setSentTime(outgoingBuffer, _clock.currentMillis());
		return true;
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
				int recvdAmount = ZmqSocketOperations.doRecv(_socket, eventBuffer, cursor, eventBuffer.length - cursor, isBlocking);
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
