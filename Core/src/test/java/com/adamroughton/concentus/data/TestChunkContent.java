package com.adamroughton.concentus.data;

import org.junit.Test;

public abstract class TestChunkContent {

	@Test
	public void empty() {
		testWith();
	}
	
	@Test
	public void singleChunk() {
		testWith(chunk(100));
	}
	
	@Test
	public void singleZeroLengthChunk() {
		testWith(chunk(0));
	}
	
	@Test
	public void multiChunk() {
		testWith(chunk(25), chunk(16), chunk(8));
	}
	
	@Test
	public void multiChunkWithZeroContentSegments() {
		testWith(chunk(90), chunk(0), chunk(5));
	}
	
	@Test
	public void multiChunkWithZeroAtEnd() {
		testWith(chunk(100), chunk(8), chunk(0));
	}
	
	protected abstract void testWith(Chunk...chunks);
	
	protected Chunk chunk(int length) {
		return chunk(length, new byte[length]);
	}
	
	protected Chunk chunk(int length, byte[] content) {
		Chunk c = new Chunk();
		c.length = length;
		c.content = content;
		return c;
	}
	
	protected static class Chunk {
		public int length;
		public byte[] content;
	}
	
}
