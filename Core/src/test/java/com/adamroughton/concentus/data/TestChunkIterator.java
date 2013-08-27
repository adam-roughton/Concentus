package com.adamroughton.concentus.data;

import static org.junit.Assert.*;

public class TestChunkIterator extends TestChunkContent {
		
	protected void testWith(Chunk...chunks) {
		ArrayBackedResizingBuffer buffer = new ArrayBackedResizingBuffer(32);
		int cursor = 0;
		
		for (Chunk chunk : chunks) {
			buffer.writeInt(cursor, chunk.length);
			cursor += ResizingBuffer.INT_SIZE;
			buffer.writeBytes(cursor, chunk.content);
			cursor += chunk.content.length;
		}
		buffer.writeInt(cursor, -1);
		
		int i = 0;
		ChunkReader reader = new ChunkReader(buffer);
		for (byte[] chunk : reader) {
			assertEquals("Failed on chunk " + i, chunks[i].length, chunk.length);
			assertArrayEquals(chunks[i].content, chunk);
			i++;
		}
	}

}
