package com.adamroughton.concentus.data;

import java.util.Iterator;

public final class ChunkIterator implements Iterator<byte[]> {

	public final ResizingBuffer _srcBuffer;
	private int _cursor;
	private byte[] _nextChunk = null;
	
	public ChunkIterator(ResizingBuffer srcBuffer, int offset) {
		this(srcBuffer.slice(offset));
	}
	
	public ChunkIterator(ResizingBuffer srcBuffer) {
		_srcBuffer = srcBuffer;
		_cursor = 0;
	}
	
	@Override
	public boolean hasNext() {
		if (_nextChunk == null) {
			int chunkLength = _srcBuffer.readInt(_cursor);
			boolean hasNext = chunkLength >= 0;
			if (hasNext) {
				_cursor += ResizingBuffer.INT_SIZE;
				_nextChunk = _srcBuffer.readBytes(_cursor, chunkLength);
				_cursor += chunkLength;
			}
			return hasNext;
		} else {
			return true;
		}
	}

	@Override
	public byte[] next() {
		byte[] next = _nextChunk;
		_nextChunk = null;
		return next;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
}
