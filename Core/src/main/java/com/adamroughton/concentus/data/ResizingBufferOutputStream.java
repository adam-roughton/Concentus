package com.adamroughton.concentus.data;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public final class ResizingBufferOutputStream extends OutputStream {

	private final ResizingBuffer _buffer;
	private int _cursor = 0;
	
	public ResizingBufferOutputStream(ResizingBuffer buffer) {
		_buffer = Objects.requireNonNull(buffer);
	}
	
	@Override
	public void write(int b) throws IOException {
		_buffer.writeByte(_cursor, (byte) (b & 0xFF));
		_cursor++;
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {		
		_buffer.copyFrom(b, off, _cursor, len);
		_cursor += len;
	}
	
	

}
