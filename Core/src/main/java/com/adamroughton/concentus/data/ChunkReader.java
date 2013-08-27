package com.adamroughton.concentus.data;

import java.util.Iterator;

public final class ChunkReader implements Iterable<byte[]> {

	private final ResizingBuffer _backingBuffer;

	public ChunkReader(ResizingBuffer buffer, int offset) {
		this(buffer.slice(offset));
	}
	
	public ChunkReader(ResizingBuffer buffer) {
		_backingBuffer = buffer;
	}
	
	@Override
	public Iterator<byte[]> iterator() {
		return new ChunkIterator(_backingBuffer);
	}

}
