package com.adamroughton.consentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.consentus.messaging.OutgoingEventHeader;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class SendQueue {

	private final OutgoingEventHeader _header;
	private final RingBuffer<byte[]> _ringBuffer;
	
	public SendQueue(final OutgoingEventHeader header, 
			final Disruptor<byte[]> backingDisruptor) {
		_header = Objects.requireNonNull(header);
		_ringBuffer = Objects.requireNonNull(backingDisruptor).getRingBuffer();
	}
	
	public final void send(SendTask task) {
		long seq = _ringBuffer.next();
		try {
			byte[] outgoingBuffer = _ringBuffer.get(seq);
			task.write(outgoingBuffer, _header);
		} finally {
			_ringBuffer.publish(seq);
		}
	}
	
}
