package com.adamroughton.concentus.data;

import java.util.Objects;

public final class MutableResizingBufferSlice extends ResizingBufferSlice {

	public MutableResizingBufferSlice() {
		_decoratedBuffer = NullResizingBuffer.INSTANCE;
		_offset = 0;
	}
	
	public void setBackingBuffer(ResizingBuffer backingBuffer, int offset) {
		_decoratedBuffer = Objects.requireNonNull(backingBuffer);
		_offset = offset;
		reset();
	}
	
	public void unsetBackingBuffer() {
		_decoratedBuffer = NullResizingBuffer.INSTANCE;
		_offset = 0;
		reset();
	}
	
}
