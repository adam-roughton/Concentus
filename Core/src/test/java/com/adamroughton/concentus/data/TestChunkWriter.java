package com.adamroughton.concentus.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestChunkWriter extends TestChunkContent {

	@Parameters
	public static Collection<ArrayBackedResizingBuffer[]> bufferParameters() {
		
		ArrayBackedResizingBuffer bufferWithContent = new ArrayBackedResizingBuffer(512);
		int cursor = 0;
		bufferWithContent.writeInt(cursor, 42);
		cursor += 42 + 4;
		bufferWithContent.writeInt(cursor, 16);
		cursor += 16 + 4;
		bufferWithContent.writeInt(cursor, -1);
		
		return Arrays.asList(new ArrayBackedResizingBuffer[][] {
			{ new ArrayBackedResizingBuffer(0) },
			{ new ArrayBackedResizingBuffer(512) },
			{ bufferWithContent }
		});
	}
	
	private final ResizingBuffer _targetBuffer;
	
	public TestChunkWriter(ArrayBackedResizingBuffer targetBuffer) {
		_targetBuffer = targetBuffer;
	}
	
	protected void testWith(Chunk...chunks) {
		ResizingBuffer buffer = _targetBuffer.copyOf();
		ChunkWriter writer = new ChunkWriter(buffer);
		
		for (Chunk chunk : chunks) {
			ResizingBuffer chunkBuffer = writer.getChunkBuffer();
			chunkBuffer.writeBytes(0, chunk.content);
			writer.commitChunk();
		}
		writer.finish();
				
		int cursor = 0;
		for (Chunk chunk : chunks) {
			if (chunk.length == 0) {
				assertEquals(buffer.readInt(cursor), ChunkWriter.EMPTY_MARKER);
			} else {
				assertEquals(buffer.readInt(cursor), chunk.length);
			}
			cursor += ResizingBuffer.INT_SIZE;
			assertArrayEquals(buffer.readBytes(cursor, chunk.length), chunk.content);
			cursor += chunk.length;
		}
	}
	
}
