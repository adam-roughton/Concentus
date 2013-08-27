package com.adamroughton.concentus.data;

public final class ChunkWriter {

	private final MutableResizingBufferSlice _chunkBuffer = new MutableResizingBufferSlice();
	private int _currentOffset;
	
	public ChunkWriter(ResizingBuffer buffer, int offset) {
		this(buffer.slice(offset));
	}
	
	public ChunkWriter(ResizingBuffer buffer) {
		_currentOffset = 0;
		nextChunk(buffer);
	}
	
	public ResizingBuffer getChunkBuffer() {
		return _chunkBuffer;
	}
	
	public void commitChunk() {
		ResizingBuffer backingBuffer = _chunkBuffer.getParentBuffer();
		int length = _chunkBuffer.getContentSize();
		backingBuffer.writeInt(_currentOffset, length);
		_currentOffset += length + ResizingBuffer.INT_SIZE;
		nextChunk(backingBuffer);
	}
	
	public void commitChunk(byte[] chunk) {
		_chunkBuffer.writeBytes(0, chunk);
		commitChunk();
	}
	
	/**
	 * Releases the backing buffer from the chunk writer.
	 */
	public void finish() {
		_chunkBuffer.unsetBackingBuffer();
	}
	
	/**
	 * Prepares the next chunk. Also writes {@code -1} into the current chunk
	 * length field until the chunk is committed to ensure that the chunked 
	 * section is always ended correctly (-1 signals the end of the chunked section).
	 * @param backingBuffer
	 */
	private void nextChunk(ResizingBuffer backingBuffer) {
		backingBuffer.writeInt(_currentOffset, -1);
		_chunkBuffer.setBackingBuffer(backingBuffer, _currentOffset + ResizingBuffer.INT_SIZE);
	}
	
}
