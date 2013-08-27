package com.adamroughton.concentus.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestChunkWriter extends TestChunkContent {

	protected void testWith(Chunk...chunks) {
		ArrayBackedResizingBuffer buffer = new ArrayBackedResizingBuffer(32);
		ChunkWriter writer = new ChunkWriter(buffer);
		
		for (Chunk chunk : chunks) {
			ResizingBuffer chunkBuffer = writer.getChunkBuffer();
			chunkBuffer.writeBytes(0, chunk.content);
			writer.commitChunk();
		}
		writer.finish();
				
		int cursor = 0;
		for (Chunk chunk : chunks) {
			assertEquals(buffer.readInt(cursor), chunk.length);
			cursor += ResizingBuffer.INT_SIZE;
			assertArrayEquals(buffer.readBytes(cursor, chunk.length), chunk.content);
			cursor += chunk.length;
		}
	}
	
}
