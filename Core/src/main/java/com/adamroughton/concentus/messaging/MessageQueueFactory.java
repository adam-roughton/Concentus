package com.adamroughton.concentus.messaging;

import java.util.Objects;

import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.CollocatedBufferEventFactory;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.lmax.disruptor.WaitStrategy;

import static com.adamroughton.concentus.util.Util.nextPowerOf2;

public final class MessageQueueFactory<TBuffer extends ResizingBuffer>  {

	private final EventQueueFactory _queueFactory;
	private final BufferFactory<TBuffer> _bufferFactory;
	
	public MessageQueueFactory(EventQueueFactory queueFactory, BufferFactory<TBuffer> bufferFactory) {
		_queueFactory = Objects.requireNonNull(queueFactory);
		_bufferFactory = Objects.requireNonNull(bufferFactory);
	}
	
	public EventQueue<TBuffer> createSingleProducerQueue(String queueName, int queueSize, int defaultBufferSize, WaitStrategy waitStrategy) {
		CollocatedBufferEventFactory<TBuffer> collocatedBufferFactory = new CollocatedBufferEventFactory<>(nextPowerOf2(queueSize), _bufferFactory, defaultBufferSize);
		return _queueFactory.createSingleProducerQueue(queueName, collocatedBufferFactory, collocatedBufferFactory.getCount(), waitStrategy);
	}
	
	public EventQueue<TBuffer> createMultiProducerQueue(String queueName, int queueSize, int defaultBufferSize, WaitStrategy waitStrategy) {
		CollocatedBufferEventFactory<TBuffer> collocatedBufferFactory = new CollocatedBufferEventFactory<>(nextPowerOf2(queueSize), _bufferFactory, defaultBufferSize);
		return _queueFactory.createMultiProducerQueue(queueName, collocatedBufferFactory, collocatedBufferFactory.getCount(), waitStrategy);
	}
	
}
