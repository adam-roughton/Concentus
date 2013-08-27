package com.adamroughton.concentus.data;

public interface BufferFactory<TBuffer extends ResizingBuffer> {

	/**
	 * Creates a single {@link ResizingBuffer} instance.
	 * @param defaultBufferSize
	 * @return
	 */
	TBuffer newInstance(int defaultBufferSize);
	
	/**
	 * Creates an array of {@link ResizingBuffer} instances that have spatial locality in
	 * memory for their default buffers (overflow buffers will not necessarily have this locality).
	 * @param count the number of buffers to allocate
	 * @return
	 */
	TBuffer[] newCollocatedSet(int defaultBufferSize, int count);
	
}
