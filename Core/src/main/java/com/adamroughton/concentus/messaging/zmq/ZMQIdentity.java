package com.adamroughton.concentus.messaging.zmq;

import java.util.Objects;

final class ZMQIdentity {
	
	public final byte[] buffer;
	public final int offset;
	public final int length;
	
	public ZMQIdentity(byte[] identityBytes) {
		this(identityBytes, 0, identityBytes.length);
	}
	
	public ZMQIdentity(byte[] buffer, int offset, int length) {
		this.buffer = Objects.requireNonNull(buffer);
		this.offset = offset;
		this.length = length;
	}

	@Override
	public int hashCode() {
		int result = 1;
		for (int i = offset; i < offset + length; i++) {
			result = result * 31 * buffer[i];
		}
		return result;
	}
	
	public ZMQIdentity copyWithNewArray() {
		byte[] identityBytes = new byte[length];
		System.arraycopy(buffer, offset, identityBytes, 0, length);
		return new ZMQIdentity(identityBytes);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ZMQIdentity) {
			ZMQIdentity that = (ZMQIdentity) obj;
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
	
}