package com.adamroughton.consentus.messaging.patterns;

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.MessagePartBufferPolicy;
import com.adamroughton.consentus.messaging.events.ByteArrayBackedEvent;
import com.lmax.disruptor.dsl.Disruptor;

public class RouterSocketSendQueue extends SendQueueBase {

	private byte[] _socketIdCache = null;
	
	public RouterSocketSendQueue(EventProcessingHeader header,
			Disruptor<byte[]> backingDisruptor,
			int socketIdLength) {
		super(header, backingDisruptor, new MessagePartBufferPolicy(0, socketIdLength));
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
