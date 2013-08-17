package com.adamroughton.concentus.messaging;

public final class ArrayBackedResizingBufferFactory implements BufferFactory<ArrayBackedResizingBuffer> {

	@Override
	public ArrayBackedResizingBuffer newInstance(int defaultBufferSize) {
		return new ArrayBackedResizingBuffer(defaultBufferSize);
	}

	@Override
	public ArrayBackedResizingBuffer[] newCollocatedSet(int defaultBufferSize,
			int count) {
		ArrayBackedResizingBuffer[] set = new ArrayBackedResizingBuffer[count];
		for (int i = 0; i < set.length; i++) {
			set[i] = new ArrayBackedResizingBuffer(defaultBufferSize);
		}
		return set;
	}

}
