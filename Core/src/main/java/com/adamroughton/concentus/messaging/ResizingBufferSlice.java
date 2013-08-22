package com.adamroughton.concentus.messaging;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.UUID;

import com.adamroughton.concentus.model.ClientId;
import com.adamroughton.concentus.util.RunningStats;

/**
 * Acts like a {@link ResizingBuffer} with the exception that offsets are internally
 * shifted by the provided {code offset} on the backing {@linkplain ResizingBuffer}.
 * 
 * @author Adam Roughton
 *
 */
public final class ResizingBufferSlice implements ResizingBuffer {

	private final ResizingBuffer _decoratedBuffer;
	private final int _offset;
	
	private int _contentSize;
	
	public ResizingBufferSlice(ResizingBuffer backingBuffer, int offset) {
		_decoratedBuffer = Objects.requireNonNull(backingBuffer);
		_offset = offset;
	}
	
	private int getAbsOffset(long relativeOffset) {
		return (int)(_offset + relativeOffset);
	}
	
	private int updateSize(long offset, int bytesWritten) {
		int newSize = (int) (offset + bytesWritten);
		_contentSize = Math.max(_contentSize, newSize);
		return bytesWritten;
	}

	public ResizingBuffer getParentBuffer() {
		return _decoratedBuffer;
	}
	
	@Override
	public int getContentSize() {
		return _contentSize;
	}
	
	@Override
	public void reset() {
		_contentSize = 0;
	}
	
	@Override
	public void clear(int offset) {
		_decoratedBuffer.clear(getAbsOffset(offset));
	}
	
	@Override
	public void clear(int offset, int length) {
		_decoratedBuffer.clear(getAbsOffset(offset), length);
	}
	
	@Override
	public ResizingBuffer slice(int offset) {
		return _decoratedBuffer.slice(offset + _offset);
	}
	
	@Override
	public void preallocate(int offset, int requiredSize) {
		_decoratedBuffer.preallocate(getAbsOffset(offset), requiredSize);
	}

	@Override
	public void copyTo(ResizingBuffer dest) {
		_decoratedBuffer.copyTo(dest, _offset, 0, _contentSize);
	}

	@Override
	public void copyTo(ResizingBuffer dest, int srcOffset) {
		_decoratedBuffer.copyTo(dest, getAbsOffset(srcOffset), 0, _contentSize);
	}

	@Override
	public void copyTo(ResizingBuffer dest, int destOffset, int length) {
		_decoratedBuffer.copyTo(dest, _offset, destOffset, Math.min(length, _contentSize));
	}

	@Override
	public void copyTo(ResizingBuffer dest, int srcOffset, int destOffset,
			int length) {
		_decoratedBuffer.copyTo(dest, getAbsOffset(srcOffset), destOffset, Math.min(length, _contentSize));
	}

	@Override
	public void copyTo(byte[] dest, int destOffset, int length) {
		_decoratedBuffer.copyTo(dest, destOffset, _offset, Math.min(length, _contentSize));
	}

	@Override
	public void copyTo(byte[] dest, int destOffset, int srcOffset, int length) {
		_decoratedBuffer.copyTo(dest, destOffset, getAbsOffset(srcOffset), Math.min(length, _contentSize));
	}

	@Override
	public void copyFrom(byte[] src, int srcOffset, int length) {
		_decoratedBuffer.copyFrom(src, srcOffset, _offset, length);
	}

	@Override
	public void copyFrom(byte[] src, int srcOffset, int destOffset, int length) {
		_decoratedBuffer.copyFrom(src, srcOffset, getAbsOffset(destOffset), length);
	}

	@Override
	public boolean readBoolean(long offset) {
		return _decoratedBuffer.readBoolean(getAbsOffset(offset));
	}

	@Override
	public int writeBoolean(long offset, boolean value) {
		return updateSize(offset, _decoratedBuffer.writeBoolean(getAbsOffset(offset), value));
	}

	@Override
	public boolean readFlagFromByte(long fieldOffset, int flagOffset) {
		return _decoratedBuffer.readFlagFromByte(getAbsOffset(fieldOffset), flagOffset);
	}

	@Override
	public int writeFlagToByte(long fieldOffset, int flagOffset, boolean raised) {
		return updateSize(fieldOffset, _decoratedBuffer.writeFlagToByte(getAbsOffset(fieldOffset), flagOffset, raised));
	}

	@Override
	public boolean readFlag(long fieldOffset, int fieldByteLength,
			int flagOffset) {
		return _decoratedBuffer.readFlag(getAbsOffset(fieldOffset), fieldByteLength, flagOffset);
	}

	@Override
	public int writeFlag(long fieldOffset, int fieldByteLength, int flagOffset,
			boolean raised) {
		return updateSize(fieldOffset, _decoratedBuffer.writeFlag(getAbsOffset(fieldOffset), fieldByteLength, flagOffset, raised));
	}

	@Override
	public byte readByte(long offset) {
		return _decoratedBuffer.readByte(getAbsOffset(offset));
	}

	@Override
	public int writeByte(long offset, byte value) {
		return updateSize(offset, _decoratedBuffer.writeByte(getAbsOffset(offset), value));
	}

	@Override
	public byte[] readBytes(long offset, int length) {
		return _decoratedBuffer.readBytes(getAbsOffset(offset), length);
	}

	@Override
	public int writeBytes(long offset, byte[] value) {
		return updateSize(offset, _decoratedBuffer.writeBytes(getAbsOffset(offset), value));
	}
	
	@Override
	public char readChar(long offset) {
		return _decoratedBuffer.readChar(getAbsOffset(offset));
	}

	@Override
	public int writeChar(long offset, char value) {
		return updateSize(offset, _decoratedBuffer.writeChar(getAbsOffset(offset), value));
	}

	@Override
	public int read4BitUInt(long fieldOffset, int bitOffset) {
		return _decoratedBuffer.read4BitUInt(getAbsOffset(fieldOffset), bitOffset);
	}

	@Override
	public int write4BitUInt(long fieldOffset, int bitOffset, int value) {
		return updateSize(fieldOffset, _decoratedBuffer.write4BitUInt(getAbsOffset(fieldOffset), bitOffset, value));
	}

	@Override
	public short readShort(long offset) {
		return _decoratedBuffer.readShort(getAbsOffset(offset));
	}

	@Override
	public int writeShort(long offset, short value) {
		return updateSize(offset, _decoratedBuffer.writeShort(getAbsOffset(offset), value));
	}

	@Override
	public int readInt(long offset) {
		return _decoratedBuffer.readInt(getAbsOffset(offset));
	}

	@Override
	public int writeInt(long offset, int value) {
		return updateSize(offset, _decoratedBuffer.writeInt(getAbsOffset(offset), value));
	}

	@Override
	public long readLong(long offset) {
		return _decoratedBuffer.readLong(getAbsOffset(offset));
	}

	@Override
	public int writeLong(long offset, long value) {
		return updateSize(offset, _decoratedBuffer.writeLong(getAbsOffset(offset), value));
	}

	@Override
	public float readFloat(long offset) {
		return _decoratedBuffer.readFloat(getAbsOffset(offset));
	}

	@Override
	public int writeFloat(long offset, float value) {
		return updateSize(offset, _decoratedBuffer.writeFloat(getAbsOffset(offset), value));
	}

	@Override
	public double readDouble(long offset) {
		return _decoratedBuffer.readDouble(getAbsOffset(offset));
	}

	@Override
	public int writeDouble(long offset, double value) {
		return updateSize(offset, _decoratedBuffer.writeDouble(getAbsOffset(offset), value));
	}

	@Override
	public byte[] readByteSegment(long offset) {
		return _decoratedBuffer.readByteSegment(getAbsOffset(offset));
	}

	@Override
	public int writeByteSegment(long offset, byte[] src, int srcOffset, int srcLength) {
		return updateSize(offset, _decoratedBuffer.writeByteSegment(getAbsOffset(offset), src, srcOffset, srcLength));
	}

	@Override
	public int writeByteSegment(long offset, byte[] src) {
		return updateSize(offset, _decoratedBuffer.writeByteSegment(getAbsOffset(offset), src));
	}

	@Override
	public String read8BitCharString(long offset) {
		return _decoratedBuffer.read8BitCharString(getAbsOffset(offset));
	}

	@Override
	public int write8BitCharString(long offset, String value) {
		return updateSize(offset, _decoratedBuffer.write8BitCharString(getAbsOffset(offset), value));
	}

	@Override
	public String readString(long offset, String charsetName) {
		return _decoratedBuffer.readString(getAbsOffset(offset), charsetName);
	}

	@Override
	public String readString(long offset, Charset charset) {
		return _decoratedBuffer.readString(getAbsOffset(offset), charset);
	}

	@Override
	public int writeString(long offset, String value, String charsetName) {
		return updateSize(offset, _decoratedBuffer.writeString(getAbsOffset(offset), value, charsetName));
	}

	@Override
	public int writeString(long offset, String value, Charset charset) {
		return updateSize(offset, _decoratedBuffer.writeString(getAbsOffset(offset), value, charset));
	}

	@Override
	public UUID readUUID(long offset) {
		return _decoratedBuffer.readUUID(getAbsOffset(offset));
	}

	@Override
	public int writeUUID(long offset, UUID value) {
		return updateSize(offset, _decoratedBuffer.writeUUID(getAbsOffset(offset), value));
	}

	@Override
	public ClientId readClientId(long offset) {
		return _decoratedBuffer.readClientId(getAbsOffset(offset));
	}

	@Override
	public int writeClientId(long offset, ClientId clientId) {
		return updateSize(offset, _decoratedBuffer.writeClientId(getAbsOffset(offset), clientId));
	}

	@Override
	public RunningStats readRunningStats(long offset) {
		return _decoratedBuffer.readRunningStats(getAbsOffset(offset));
	}

	@Override
	public int writeRunningStats(long offset, RunningStats value) {
		return updateSize(offset, _decoratedBuffer.writeRunningStats(getAbsOffset(offset), value));
	}

	@Override
	public String toString() {
		return String.format("Slice [%d - ] of %s", _offset, _decoratedBuffer.toString());
	}
	
}
