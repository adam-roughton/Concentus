package com.adamroughton.concentus.util;

public class CircularStringBuffer {

	private final char[] _backingBuffer;
	private long _cursor;
	
	public CircularStringBuffer(int capacity) {
		_backingBuffer = new char[capacity];
		_cursor = 0;
	}
	
	public CircularStringBuffer append(String string) {
		if (string == null) string = "null";
		int bufferLength = _backingBuffer.length;
		if (bufferLength > 0) {
			int stringLength = string.length();
			for (int i = 0; i < stringLength; i++) {
				_backingBuffer[(int) (_cursor++ % bufferLength)] = string.charAt(i);
			}
		}
		return this;
	}
	
	public CircularStringBuffer append(char c) {
		int bufferLength = _backingBuffer.length;
		if (bufferLength > 0) {
			_backingBuffer[(int) (_cursor++ % _backingBuffer.length)] = c;
		}
		return this;
	}
	
	public void reset() {
		_cursor = 0;
	}
	
	public String toString() {
		int bufferLength = _backingBuffer.length;
		if (bufferLength == 0) return "";
		
		char[] contents;
		if (_cursor < bufferLength) {
			contents = new char[(int)_cursor];
			System.arraycopy(_backingBuffer, 0, contents, 0, contents.length);
		} else {
			contents = new char[bufferLength];
			int cursorIndex = (int) (_cursor % bufferLength);
			int firstSegLength = bufferLength - cursorIndex;
			System.arraycopy(_backingBuffer, cursorIndex, contents, 0, firstSegLength);
			System.arraycopy(_backingBuffer, 0, contents, firstSegLength, cursorIndex);
		}
		return new String(contents);
	}
	
}
