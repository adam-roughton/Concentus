package com.adamroughton.concentus.disruptor;

import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.ResizingBuffer;

/**
 * Allocates {@code count} collocated buffer entries using the provided {@link BufferFactory#newCollocatedSet(int, int)}
 * instance factory, then returns buffers from this pool on successive calls to {@link CollocatedBufferEventFactory#newInstance()}. 
 * When the pool is exhausted, new non-collocated buffers are created using {@link BufferFactory#newInstance(int)}.
 * @author Adam Roughton
 *
 * @param <TBuffer>
 */
public final class CollocatedBufferEventFactory<TBuffer extends ResizingBuffer> implements EventEntryHandler<TBuffer> {

	private final int _count;
	private final BufferFactory<TBuffer> _bufferFactory;
	private final int _defaultBufferSize;
	private final TBuffer[] _collocatedBuffers;
	
	private int _bufferIndex = 0;
	
	public CollocatedBufferEventFactory(int count, BufferFactory<TBuffer> bufferFactory, int defaultBufferSize) {
		_count = count;
		_bufferFactory = bufferFactory;
		_defaultBufferSize = defaultBufferSize;
		_collocatedBuffers = bufferFactory.newCollocatedSet(defaultBufferSize, count);
	}
	
	@Override
	public TBuffer newInstance() {
		if (remainingBufferCount() > 0) {
			return _collocatedBuffers[_bufferIndex++];			
		} else {
			return _bufferFactory.newInstance(_defaultBufferSize);
		}
	}
	
	public int getCount() {
		return _count;
	}
	
	public int remainingBufferCount() {
		return _collocatedBuffers.length - _bufferIndex;
	}

	@Override
	public void clear(TBuffer event) {
		event.reset();
	}

	@Override
	public void copy(TBuffer source, TBuffer destination) {
		source.copyTo(destination);
	}

}
