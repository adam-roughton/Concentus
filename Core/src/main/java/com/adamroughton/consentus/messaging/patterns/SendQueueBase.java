package com.adamroughton.consentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;

public abstract class SendQueueBase {

	private final EventProcessingHeader _header;
	private final Disruptor<byte[]> _disruptor;
	private final MessageFrameBufferMapping _mapping;
	
	public SendQueueBase(final EventProcessingHeader header, 
			final Disruptor<byte[]> backingDisruptor, 
			final MessageFrameBufferMapping mapping) {
		_header = Objects.requireNonNull(header);
		_disruptor = Objects.requireNonNull(backingDisruptor);
		_mapping = Objects.requireNonNull(mapping);
	}
	
	public MessageFrameBufferMapping getMessagePartPolicy() {
		return _mapping;
	}
	
	protected <TEvent extends ByteArrayBackedEvent> void doSend(
			final TEvent eventHelper, 
			final EventWriter<TEvent> eventWriter) {
		_disruptor.publishEvent(new EventTranslator<byte[]>() {
			
			@Override
			public void translateTo(byte[] event, long sequence) {
				int contentOffset = _header.getEventOffset();
				try {
					for (int i = 0; i < _mapping.partCount(); i++) {
						int offset = _mapping.getOffset(i) + contentOffset;
						
						if (i == _mapping.partCount() - 1) {
							try {
								eventHelper.setBackingArray(event, offset);
								boolean isValid = eventWriter.write(eventHelper, sequence);
								_header.setIsValid(isValid, event);
							} finally {
								eventHelper.releaseBackingArray();
							}
						} else {
							int length = _mapping.getIntermediatePartLength(i);
							writeMessagePart(i, event, offset, length);
						}
					}
				} catch (Exception e) {
					Log.error("Error sending event", e);
					_header.setIsValid(false, event);
				}
			}
		});
	}
	
	protected abstract void writeMessagePart(int partIndex, 
			byte[] event, int offset, int length) throws Exception;
	
}
