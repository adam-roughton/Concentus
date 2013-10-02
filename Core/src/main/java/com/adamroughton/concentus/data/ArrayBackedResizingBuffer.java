package com.adamroughton.concentus.data;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.util.RunningStats;
import com.adamroughton.concentus.util.Util;

public final class ArrayBackedResizingBuffer implements ResizingBuffer {
	
	/**
	 * The default allocated message buffer for the container.
	 */
	private final byte[] _buffer;
	
	/**
	 * The buffer allocated if the message exceeds the default size.
	 */
	private byte[] _overflowBuffer;
	
	private int _contentSize;
	
	public ArrayBackedResizingBuffer(byte[] buffer) {
		_buffer = Objects.requireNonNull(buffer);
		_contentSize = buffer.length;
	}
	
	public ArrayBackedResizingBuffer(int defaultSize) {
		_buffer = new byte[defaultSize];
	}
	
	private int sanityCheck(int requestedSize) {
		if (requestedSize > Constants.MAX_BYTE_ARRAY_SIZE)
			throw new IllegalArgumentException(String.format("The requested array " +
					"size %d exceeded the maximum array size setting of %d.", requestedSize, 
					Constants.MAX_BYTE_ARRAY_SIZE));
		return requestedSize;
	}
	
	/**
	 * Gets the size of the content currently stored in this buffer.
	 * @return
	 */
	public int getContentSize() {
		return _contentSize;
	}
	
	@Override
	public ResizingBuffer slice(int offset) {
		return new ResizingBufferSlice(this, offset);
	}
	
	/**
	 * Gets the array currently backing the message stored in this container.
	 * @return
	 */
	public byte[] getBuffer() {
		return _overflowBuffer == null? _buffer : _overflowBuffer;
	}
	
	public byte[] allocateForWriting(int requiredSize) {
		sanityCheck(requiredSize);
		reset();
		_contentSize = requiredSize;
		if (_buffer.length < requiredSize) {
			_overflowBuffer = new byte[requiredSize];
			return _overflowBuffer;
		} else {
			return _buffer;
		}
	}
	
	public void reset() {
		_overflowBuffer = null;
		BytesUtil.clear(_buffer, 0, _buffer.length);
		_contentSize = 0;
	}
	
	@Override
	public void reset(int offset) {
		clear(offset);
		_contentSize = offset;
	}
	
	public void clear(int offset) {
		clear(offset, _contentSize - offset);
	}
	
	public void clear(int offset, int length) {
		int prevContentSize = _contentSize;
		BytesUtil.clear(ensureSize(offset, length), offset, length);
		if (offset + length > prevContentSize) {
			_contentSize = offset;
		}
	}
	
	private byte[] ensureSize(int size) {
		_contentSize = Math.max(_contentSize, size);
		byte[] buffer = getBuffer();
		if (buffer.length < size) {
			_overflowBuffer = increaseSize(buffer, size);
			return _overflowBuffer;
		} else {
			return buffer;
		}
	}
	
	private byte[] ensureSize(long offset, int dataSize) {
		return ensureSize((int) offset + dataSize);
	}
	
	private byte[] increaseSize(byte[] original, int requiredSize) {
		byte[] newArray = new byte[sanityCheck(Math.max(original.length * 2, Util.nextPowerOf2(requiredSize)))];
		System.arraycopy(original, 0, newArray, 0, original.length);
		return newArray;
	}
	
	@Override
	public void preallocate(int offset, int requiredSize) {
		sanityCheck(requiredSize + offset);
		BytesUtil.clear(ensureSize(offset, requiredSize), offset, requiredSize);
	}
	
	public void copyTo(ResizingBuffer dest) {
		copyTo(dest, 0, 0, _contentSize);
	}
	
	public void copyTo(ResizingBuffer dest, int srcOffset) {
		copyTo(dest, srcOffset, 0, _contentSize - srcOffset);
	}
	
	@Override
	public void copyTo(ResizingBuffer dest, int destOffset, int length) {
		copyTo(dest, 0, destOffset, length);
	}

	@Override
	public void copyTo(ResizingBuffer dest, int srcOffset, int destOffset,
			int length) {
		byte[] src = getBuffer();
		dest.copyFrom(src, srcOffset, destOffset, Math.min(src.length - srcOffset, length));		
	}
	
	@Override
	public void copyTo(byte[] dest, int destOffset, int srcOffset, int length) {
		byte[] src = getBuffer();
		System.arraycopy(src, srcOffset, dest, destOffset, Math.min(src.length - srcOffset, length));
	}
	
	public void copyTo(byte[] dest, int destOffset, int length) {
		copyTo(dest, destOffset, 0, length);
	}
	
	@Override
	public void copyFrom(byte[] src, int srcOffset, int destOffset, int length) {
		System.arraycopy(src, srcOffset, ensureSize(destOffset, length), destOffset, length);
	}
	
	public void copyFrom(byte[] src, int srcOffset, int length) {
		copyFrom(src, 0, srcOffset, length);
	}
	
	@Override
	public ResizingBuffer copyOf() {
		return copyOf(0, _contentSize);
	}

	@Override
	public ResizingBuffer copyOf(int offset, int length) {
		ArrayBackedResizingBuffer newBuffer = new ArrayBackedResizingBuffer(_buffer.length);
		copyTo(newBuffer, offset, 0, length);
		return newBuffer;
	}
	
	public boolean readBoolean(long offset) {
		return BytesUtil.readBoolean(ensureSize(offset, BOOL_SIZE), offset);
	}
	
	public int writeBoolean(long offset, boolean value) {
		BytesUtil.writeBoolean(ensureSize(offset, BOOL_SIZE), offset, value);
		return BOOL_SIZE;
	}
	
	public boolean readFlagFromByte(long fieldOffset, int flagOffset) {	
		return BytesUtil.readFlagFromByte(ensureSize(fieldOffset, 1), fieldOffset, flagOffset);
	}
	
	public int writeFlagToByte(long fieldOffset, int flagOffset, boolean raised) {
		BytesUtil.writeFlagToByte(ensureSize(fieldOffset, 1), fieldOffset, flagOffset, raised);
		return 1;
	}
	
	public boolean readFlag(long fieldOffset, int fieldByteLength, int flagOffset) {
		return BytesUtil.readFlag(ensureSize(fieldOffset, fieldByteLength), fieldOffset, fieldByteLength, flagOffset);
	}
	
	public int writeFlag(long fieldOffset, int fieldByteLength, int flagOffset, boolean raised) {
		BytesUtil.writeFlag(ensureSize(fieldOffset, fieldByteLength), fieldOffset, fieldByteLength, flagOffset, raised);
		return fieldByteLength;
	}
	
	@Override
	public byte readByte(long offset) {
		return ensureSize(offset, 1)[(int) offset];
	}

	@Override
	public int writeByte(long offset, byte value) {
		ensureSize(offset, 1)[(int) offset] = value;
		return 1;
	}

	@Override
	public byte[] readBytes(long offset, int length) {
		byte[] retVal = new byte[sanityCheck(length)];
		copyTo(retVal, 0, (int) offset, length);
		return retVal;
	}

	@Override
	public int writeBytes(long offset, byte[] value) {
		copyFrom(value, 0, (int) offset, value.length);
		return value.length;
	}
	
	public char readChar(long offset) {
		return BytesUtil.readChar(ensureSize(offset, CHAR_SIZE), offset);
	}
	
	public int writeChar(long offset, char value) {
		BytesUtil.writeChar(ensureSize(offset, CHAR_SIZE), offset, value);
		return CHAR_SIZE;
	}
	
	public int read4BitUInt(long fieldOffset, int bitOffset) {
		return BytesUtil.read4BitUInt(ensureSize(fieldOffset, 1), fieldOffset, bitOffset);
	}
	
	public int write4BitUInt(long fieldOffset, int bitOffset, int value) {
		BytesUtil.write4BitUInt(ensureSize(fieldOffset, 1), fieldOffset, bitOffset, value);
		return 1;
	}
	
	public short readShort(long offset) {
		return BytesUtil.readShort(ensureSize(offset, SHORT_SIZE), offset);
	}
	
	public int writeShort(long offset, short value) {
		BytesUtil.writeShort(ensureSize(offset, SHORT_SIZE), offset, value);
		return SHORT_SIZE;
	}
	
	public int readInt(long offset) {
		return BytesUtil.readInt(ensureSize(offset, INT_SIZE), offset);
	}
	
	public int writeInt(long offset, int value) {
		BytesUtil.writeInt(ensureSize(offset, INT_SIZE), offset, value);
		return INT_SIZE;
	}
	
	public long readLong(long offset) {
		return BytesUtil.readLong(ensureSize(offset, LONG_SIZE), offset);
	}
	
	public int writeLong(long offset, long value) {
		BytesUtil.writeLong(ensureSize(offset, LONG_SIZE), offset, value);
		return LONG_SIZE;
	}
	
	public float readFloat(long offset) {
		return BytesUtil.readFloat(ensureSize(offset, FLOAT_SIZE), offset);
	}
	
	public int writeFloat(long offset, float value) {
		BytesUtil.writeFloat(ensureSize(offset, FLOAT_SIZE), offset, value);
		return FLOAT_SIZE;
	}
	
	public double readDouble(long offset) {
		return BytesUtil.readDouble(ensureSize(offset, DOUBLE_SIZE), offset);
	}
	
	public int writeDouble(long offset, double value) {
		BytesUtil.writeDouble(ensureSize(offset, DOUBLE_SIZE), offset, value);
		return DOUBLE_SIZE;
	}
	
	public byte[] readByteSegment(long offset) {
		return BytesUtil.readBytes(ensureSize(offset, 4), offset);
	}
	
	public int writeByteSegment(long offset, byte[] src, int srcOffset, int srcLength) {
		return BytesUtil.writeBytes(ensureSize(offset, srcLength + 4), offset, src, srcOffset, srcLength);
	}
	
	public int writeByteSegment(long offset, byte[] src) {
		return BytesUtil.writeBytes(ensureSize(offset, src.length + 4), offset, src, 0, src.length);
	}
	
	public String read8BitCharString(long offset) {
		return readString(offset, StandardCharsets.ISO_8859_1);
	}
	
	public int write8BitCharString(long offset, String value) {
		return writeString(offset, value, StandardCharsets.ISO_8859_1);
	}
	
	public String readString(long offset, String charsetName) {
		return readString(offset, Charset.forName(charsetName));
	}
	
	public String readString(long offset, Charset charset) {
		return new String(BytesUtil.readBytes(ensureSize(offset, 4), offset), charset);
	}
	
	public int writeString(long offset, String value, String charsetName) {
		return writeString(offset, value, Charset.forName(charsetName));
	}
	
	public int writeString(long offset, String value, Charset charset) {
		byte[] stringBytes = value.getBytes(charset);
		return BytesUtil.writeBytes(ensureSize(offset, stringBytes.length + 4), offset, stringBytes);
	}
	
	public UUID readUUID(long offset) {
		return BytesUtil.readUUID(ensureSize(offset, UUID_SIZE), offset);
	}
	
	public int writeUUID(long offset, UUID value) {
		BytesUtil.writeUUID(ensureSize(offset, UUID_SIZE), offset, value);
		return UUID_SIZE;
	}
	
	public ClientId readClientId(long offset) {
		return BytesUtil.readClientId(ensureSize(offset, CLIENT_ID_SIZE), offset);
	}
	
	public int writeClientId(long offset, final ClientId clientId) {
		BytesUtil.writeClientId(ensureSize(offset, CLIENT_ID_SIZE), offset, clientId);
		return CLIENT_ID_SIZE;
	}
	
	public RunningStats readRunningStats(long offset) {
		return BytesUtil.readRunningStats(ensureSize(offset, RUNNING_STATS_SIZE), offset);
	}
	
	public int writeRunningStats(long offset, RunningStats value) {
		BytesUtil.writeRunningStats(ensureSize(offset, RUNNING_STATS_SIZE), offset, value);
		return RUNNING_STATS_SIZE;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		boolean isFirst = true;
		
		byte[] buffer = getBuffer();
		int length = Math.min(buffer.length, _contentSize);
		
		for (int i = 0; i < length; i++) {
			if (isFirst) {
				isFirst = false;
			} else {
				sb.append(", ");
			}
			sb.append(buffer[i]);
		}
		sb.append("]");
		return sb.toString();
	}

	@Override
	public int contentHashCode() {
		return Arrays.hashCode(getBuffer());
	}

	@Override
	public int contentHashCode(long offset, int length) {
		int iOffset = (int) offset;
		byte[] buffer = ensureSize(offset, length);
		int result = 1;
		for (int i = iOffset; i < iOffset + length; i++) {
			result = result * 31 * buffer[i];
		}
		return result;
	}

	@Override
	public boolean contentEquals(ResizingBuffer other) {
		if (other instanceof ArrayBackedResizingBuffer) {
			ArrayBackedResizingBuffer otherArrayBackedBuffer = (ArrayBackedResizingBuffer) other;
			return Arrays.equals(this.getBuffer(), otherArrayBackedBuffer.getBuffer());
		} else {
			return contentEqualsDifferentBufferTypes(other);
		}
	}

	@Override
	public boolean contentEquals(ResizingBuffer other, long thisOffset,
			long otherOffset, int length) {
		if (other instanceof ArrayBackedResizingBuffer) {
			byte[] thisBuffer = this.getBuffer();
			byte[] otherBuffer = ((ArrayBackedResizingBuffer) other).getBuffer();
			int iThisOffset = (int) thisOffset;
			int iOtherOffset = (int) otherOffset;
			
			if (thisBuffer.length < length + thisOffset || otherBuffer.length < length + otherOffset)
				return false;
			for (int i = 0; i < length; i++) {
				if (thisBuffer[iThisOffset + i] != otherBuffer[iOtherOffset + i])
					return false;
			}
			return true;
		} else {
			return contentEqualsDifferentBufferTypes(other, thisOffset, otherOffset, length);
		}
	}
	
	private boolean contentEqualsDifferentBufferTypes(ResizingBuffer other) {
		return contentEqualsDifferentBufferTypes(other, 0, 0, _contentSize);
	}
	
	private boolean contentEqualsDifferentBufferTypes(ResizingBuffer other, long thisOffset, long otherOffset, int length) {
		if (this.getContentSize() < length + thisOffset || other.getContentSize() < length + otherOffset)
			return false;

		int longCount = length / 8;
		for (int i = 0; i < longCount; i++) {
			if (this.readLong(thisOffset + i * LONG_SIZE) != other.readLong(otherOffset + i * LONG_SIZE))
				return false;
		}
		int remainder = length % 8;
		int cursor = longCount * LONG_SIZE;
		for (int i = 0; i < remainder; i++) {
			if (this.readByte(thisOffset + i + cursor) != other.readByte(otherOffset + i + cursor))
				return false;
		}
		return true;
	}
	
}
