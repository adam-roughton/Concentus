package com.adamroughton.concentus.messaging;

import java.util.Arrays;
import java.util.Objects;

public final class SocketIdentity {
	
	public final byte[] buffer;
	public final int offset;
	public final int length;
	private int _hashCodeCache = -1;
	
	public SocketIdentity(byte[] identityBytes) {
		this(identityBytes, 0, identityBytes.length);
	}
	
	public SocketIdentity(byte[] buffer, int offset, int length) {
		this.buffer = Objects.requireNonNull(buffer);
		this.offset = offset;
		this.length = length;
	}

	@Override
	public int hashCode() {
		if (_hashCodeCache == -1) {
			int result = 1;
			for (int i = offset; i < offset + length; i++) {
				result = result * 31 * buffer[i];
			}
			_hashCodeCache = result;
		}
		return _hashCodeCache;
	}
	
	public SocketIdentity copyWithNewArray() {
		byte[] identityBytes = new byte[length];
		System.arraycopy(buffer, offset, identityBytes, 0, length);
		return new SocketIdentity(identityBytes);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SocketIdentity) {
			SocketIdentity that = (SocketIdentity) obj;
			if (this.length != that.length)
				return false;
			for (int i = 0; i < this.length; i++) {
				if (this.buffer[this.offset + i] != that.buffer[that.offset + i])
					return false;
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "SocketIdentity [buffer=" + Arrays.toString(buffer)
				+ ", offset=" + offset + ", length=" + length + "]";
	}
	
	
	
}