package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;
import com.lmax.disruptor.dsl.Disruptor;

import static com.adamroughton.consentus.messaging.patterns.Patterns.validate;

public class RouterSendQueueWriter extends SendQueueBase {

	private byte[] _socketIdCache = null;
	
	public RouterSendQueueWriter(EventProcessingHeader header, 
			Disruptor<byte[]> backingDisruptor, 
			MessageFrameBufferMapping mapping) {
		super(header, backingDisruptor, validate(mapping, 2));
	}
	
	public RouterSendQueueWriter(EventProcessingHeader header,
			Disruptor<byte[]> backingDisruptor,
			int socketIdLength) {
		super(header, backingDisruptor, new MessageFrameBufferMapping(0, socketIdLength));
	}
	
	public <TEvent extends ByteArrayBackedEvent> void send(byte[] socketId, TEvent eventHelper, EventWriter<TEvent> writer) {
		_socketIdCache = socketId;
		doSend(eventHelper, writer);
		_socketIdCache = null;
	}

	@Override
	protected void writeMessagePart(int partIndex,
			byte[] event, int offset, int length) throws Exception {
		if (partIndex == 0) {
			System.arraycopy(_socketIdCache, 0, event, offset, _socketIdCache.length);
		}
	}

}
