package com.adamroughton.concentus.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public final class ResizingBufferInputStream extends InputStream {

	private final ResizingBuffer _buffer;
	private int _cursor;
	private int _mark;
	
	public ResizingBufferInputStream(ResizingBuffer buffer) {
		_buffer = Objects.requireNonNull(buffer);
		_cursor = 0;
		_mark = 0;
	}
	
	@Override
	public int read() throws IOException {
		return _buffer.readByte(_cursor++) & 0xFF;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {		
		int lenToRead = Math.min(len, remaining());
		if (lenToRead == 0) {
			return -1;
		} else {
			_buffer.copyTo(b, off, _cursor, lenToRead);
			_cursor += lenToRead;
			return lenToRead;
		}
	}

	@Override
	public long skip(long n) throws IOException {
		long skipCount = Math.min(n, remaining());
		_cursor += skipCount;
		return skipCount;
	}

	@Override
	public int available() throws IOException {
		return remaining();
	}
	
	private int remaining() {
		return Math.max(0, _buffer.getContentSize() - _cursor);
	}

	@Override
	public synchronized void mark(int readlimit) {
		_mark = _cursor;
	}

	@Override
	public synchronized void reset() throws IOException {
		_cursor = _mark;
	}

	@Override
	public boolean markSupported() {
		return true;
	}
	
	

}
