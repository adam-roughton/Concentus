package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;

import static com.adamroughton.consentus.messaging.patterns.Patterns.validate;

public class RouterRecvQueueReader extends RecvQueueReader {

	private final int _socketIdOffset;
	private final int _socketIdLength;
	
	public RouterRecvQueueReader(final EventProcessingHeader header, final MessageFrameBufferMapping mapping) {
		super(header, validate(mapping, 2), 1);
		_socketIdOffset = header.getEventOffset() + mapping.getOffset(0);
		_socketIdLength = mapping.getIntermediatePartLength(0);
	}
	
	public RouterRecvQueueReader(final EventProcessingHeader header, final int socketIdLength) {
		super(header, new MessageFrameBufferMapping(0, socketIdLength), 1);
		_socketIdOffset = header.getEventOffset();
		_socketIdLength = socketIdLength;
	}
	
	public byte[] getSocketId(byte[] eventBytes) {
		byte[] senderId = new byte[_socketIdLength];
		System.arraycopy(eventBytes, _socketIdOffset, senderId, 0, _socketIdLength);
		return senderId;
	}
	
	public void copySocketId(byte[] eventBytes, byte[] dest, int offset, int length) {
		System.arraycopy(eventBytes, _socketIdOffset, dest, offset, length > _socketIdLength? _socketIdLength : length);
	}

}
