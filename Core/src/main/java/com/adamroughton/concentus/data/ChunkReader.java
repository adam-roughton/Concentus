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
	
	public Iterable<ResizingBuffer> asBuffers() {
		return new Iterable<ResizingBuffer>() {
			
			@Override
			public Iterator<ResizingBuffer> iterator() {
				final Iterator<byte[]> backingIterator = ChunkReader.this.iterator();
				return new Iterator<ResizingBuffer>() {

					@Override
					public boolean hasNext() {
						return backingIterator.hasNext();
					}

					@Override
					public ResizingBuffer next() {
						return new ArrayBackedResizingBuffer(backingIterator.next());
					}

					@Override
					public void remove() {
						backingIterator.remove();
					}
				};
			}
		};
	}

}
