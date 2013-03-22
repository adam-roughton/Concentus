package com.adamroughton.consentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;

public class RecvQueueReader {

	private final EventProcessingHeader _header;
	private final MessageFrameBufferMapping _mapping;
	private final int _contentOffset;
	
	public RecvQueueReader(final EventProcessingHeader header, 
			final MessageFrameBufferMapping mapping,
			final int contentPartIndex) {
		_header = Objects.requireNonNull(header);
		_mapping = Objects.requireNonNull(mapping);
		_contentOffset = _header.getEventOffset() + mapping.getOffset(contentPartIndex);
	}
	
	public boolean isValid(byte[] eventBytes) {
		return _header.isValid(eventBytes);
	}
	
	public int getEventType(byte[] eventBytes) {
		return MessageBytesUtil.readInt(eventBytes, _contentOffset);
	}
	
	/**
	 * Reads the given event bytes using the provided event helper and reader.
	 * @param eventBytes the event bytes to process
	 * @param eventHelper the helper to use to translate the event bytes
	 * @param reader the reader to use to extract the event data
	 * @return {@code true} if the event was valid (i.e. the header signalled the event should be processed); {@code false} otherwise
	 */
	public <TEvent extends ByteArrayBackedEvent> boolean read(byte[] eventBytes, TEvent eventHelper, EventReader<TEvent> reader) {
		if (!_header.isValid(eventBytes)) {
			return false;
		}
		eventHelper.setBackingArray(eventBytes, _contentOffset);
		try {
			reader.read(eventHelper);
		} finally {
			eventHelper.releaseBackingArray();
		}
		return true;
	}
	
	public MessageFrameBufferMapping getMessageFrameBufferMapping() {
		return _mapping;
	}
	
}
