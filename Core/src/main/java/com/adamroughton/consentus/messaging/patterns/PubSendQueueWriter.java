package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.dsl.Disruptor;

public class PubSendQueueWriter extends SendQueueBase {
	
	private EventType _eventTypeCache = null;
	
	public PubSendQueueWriter(final EventProcessingHeader header,
			final Disruptor<byte[]> backingDisruptor) {
		super(header, backingDisruptor, new MessageFrameBufferMapping(0, 4));
	}
	
	public <TEvent extends ByteArrayBackedEvent> void send(EventType eventType, TEvent eventHelper, EventWriter<TEvent> writer) {
		_eventTypeCache = eventType;
		doSend(eventHelper, writer);
		_eventTypeCache = null;
	}

	@Override
	protected void writeMessagePart(int partIndex,
			byte[] event, int offset, int length) throws Exception {
		if (partIndex == 0) {
			MessageBytesUtil.writeInt(event, offset, _eventTypeCache.getId());
		}
	}
	
}
