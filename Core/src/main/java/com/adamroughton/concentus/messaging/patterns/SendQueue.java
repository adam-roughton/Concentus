package com.adamroughton.concentus.messaging.patterns;

import java.util.Objects;

import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;

public class SendQueue<TSendHeader extends OutgoingEventHeader> {

	private final TSendHeader _header;
	private final RingBuffer<byte[]> _ringBuffer;
	
	public SendQueue(final TSendHeader header, 
			final RingBuffer<byte[]> buffer) {
		_header = Objects.requireNonNull(header);
		_ringBuffer = Objects.requireNonNull(buffer);
	}
	
	public final void send(SendTask<TSendHeader> task) {
		long seq;
		try {
			seq = _ringBuffer.tryNext(1);
		} catch (InsufficientCapacityException eNoCapacity) {
			throw new RuntimeException(eNoCapacity);
		}
		try {
			byte[] outgoingBuffer = _ringBuffer.get(seq);
			task.write(outgoingBuffer, _header);
		} finally {
			_ringBuffer.publish(seq);
		}
	}
	
}
