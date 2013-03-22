package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;

public class SubRecvQueueReader extends RecvQueueReader {

	private final int _subIdOffset;
	
	public SubRecvQueueReader(final EventProcessingHeader header) {
		super(header, new MessageFrameBufferMapping(0, 4), 1);
		_subIdOffset = header.getEventOffset();
	}
	
	public int getSubId(byte[] eventBytes) {
		return MessageBytesUtil.readInt(eventBytes, _subIdOffset);
	}

}
