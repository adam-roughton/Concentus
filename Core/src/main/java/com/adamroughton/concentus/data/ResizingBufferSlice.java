package com.adamroughton.concentus.data;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.UUID;

import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.util.RunningStats;

/**
 * Acts like a {@link ResizingBuffer} with the exception that offsets are internally
 * shifted by the provided {code offset} on the backing {@linkplain ResizingBuffer}.
 * 
 * @author Adam Roughton
 *
 */
public class ResizingBufferSlice implements ResizingBuffer {

	protected ResizingBuffer _decoratedBuffer;
	protected int _offset;
	
	protected ResizingBufferSlice() {
	}
	
	public ResizingBufferSlice(ResizingBuffer backingBuffer, int offset) {
		_decoratedBuffer = Objects.requireNonNull(backingBuffer);
		_offset = offset;
	}
	
	private int getAbsOffset(long relativeOffset) {
		return (int)(_offset + relativeOffset);
	}

	public final ResizingBuffer getParentBuffer() {
		return _decoratedBuffer;
	}
	
	@Override
	public final int getContentSize() {
		return Math.max(0, _decoratedBuffer.getContentSize() - _offset);
	}
	
	@Override
	public final void reset() {
		reset(0);
	}
	
	@Override
	public final void reset(int offset) {
		_decoratedBuffer.reset(getAbsOffset(offset));
	}
	
	@Override
	public final void clear(int offset) {
		_decoratedBuffer.clear(getAbsOffset(offset));
	}
	
	@Override
	public final void clear(int offset, int length) {	
		_decoratedBuffer.clear(getAbsOffset(offset), length);
	}
	
	@Override
	public final ResizingBuffer slice(int offset) {
		return _decoratedBuffer.slice(offset + _offset);
	}
	
	@Override
	public final void preallocate(int offset, int requiredSize) {
		_decoratedBuffer.preallocate(getAbsOffset(offset), requiredSize);
	}

	@Override
	public final void copyTo(ResizingBuffer dest) {
		_decoratedBuffer.copyTo(dest, _offset, 0, getContentSize());
	}

	@Override
	public final void copyTo(ResizingBuffer dest, int srcOffset) {
		_decoratedBuffer.copyTo(dest, getAbsOffset(srcOffset), 0, getContentSize());
	}

	@Override
	public final void copyTo(ResizingBuffer dest, int destOffset, int length) {
		_decoratedBuffer.copyTo(dest, _offset, destOffset, Math.min(length, getContentSize()));
	}

	@Override
	public final void copyTo(ResizingBuffer dest, int srcOffset, int destOffset,
			int length) {
		_decoratedBuffer.copyTo(dest, getAbsOffset(srcOffset), destOffset, Math.min(length, getContentSize()));
	}

	@Override
	public final void copyTo(byte[] dest, int destOffset, int length) {
		_decoratedBuffer.copyTo(dest, destOffset, _offset, Math.min(length, getContentSize()));
	}

	@Override
	public final void copyTo(byte[] dest, int destOffset, int srcOffset, int length) {
		_decoratedBuffer.copyTo(dest, destOffset, getAbsOffset(srcOffset), Math.min(length, getContentSize()));
	}

	@Override
	public final void copyFrom(byte[] src, int srcOffset, int length) {
		copyFrom(src, srcOffset, 0, length);
	}

	@Override
	public final void copyFrom(byte[] src, int srcOffset, int destOffset, int length) {
		_decoratedBuffer.copyFrom(src, srcOffset, getAbsOffset(destOffset), length);
	}
	
	@Override
	public final ResizingBuffer copyOf() {
		return copyOf(0, getContentSize());
	}

	@Override
	public final ResizingBuffer copyOf(int offset, int length) {
		return _decoratedBuffer.copyOf(getAbsOffset(offset), length);
	}

	@Override
	public final boolean readBoolean(long offset) {
		return _decoratedBuffer.readBoolean(getAbsOffset(offset));
	}

	@Override
	public final int writeBoolean(long offset, boolean value) {
		return _decoratedBuffer.writeBoolean(getAbsOffset(offset), value);
	}

	@Override
	public final boolean readFlagFromByte(long fieldOffset, int flagOffset) {
		return _decoratedBuffer.readFlagFromByte(getAbsOffset(fieldOffset), flagOffset);
	}

	@Override
	public final int writeFlagToByte(long fieldOffset, int flagOffset, boolean raised) {
		return _decoratedBuffer.writeFlagToByte(getAbsOffset(fieldOffset), flagOffset, raised);
	}

	@Override
	public final boolean readFlag(long fieldOffset, int fieldByteLength,
			int flagOffset) {
		return _decoratedBuffer.readFlag(getAbsOffset(fieldOffset), fieldByteLength, flagOffset);
	}

	@Override
	public final int writeFlag(long fieldOffset, int fieldByteLength, int flagOffset,
			boolean raised) {
		return _decoratedBuffer.writeFlag(getAbsOffset(fieldOffset), fieldByteLength, flagOffset, raised);
	}

	@Override
	public final byte readByte(long offset) {
		return _decoratedBuffer.readByte(getAbsOffset(offset));
	}

	@Override
	public final int writeByte(long offset, byte value) {
		return _decoratedBuffer.writeByte(getAbsOffset(offset), value);
	}

	@Override
	public final byte[] readBytes(long offset, int length) {
		return _decoratedBuffer.readBytes(getAbsOffset(offset), length);
	}

	@Override
	public final int writeBytes(long offset, byte[] value) {
		return _decoratedBuffer.writeBytes(getAbsOffset(offset), value);
	}
	
	@Override
	public final char readChar(long offset) {
		return _decoratedBuffer.readChar(getAbsOffset(offset));
	}

	@Override
	public final int writeChar(long offset, char value) {
		return _decoratedBuffer.writeChar(getAbsOffset(offset), value);
	}

	@Override
	public final int read4BitUInt(long fieldOffset, int bitOffset) {
		return _decoratedBuffer.read4BitUInt(getAbsOffset(fieldOffset), bitOffset);
	}

	@Override
	public final int write4BitUInt(long fieldOffset, int bitOffset, int value) {
		return _decoratedBuffer.write4BitUInt(getAbsOffset(fieldOffset), bitOffset, value);
	}

	@Override
	public final short readShort(long offset) {
		return _decoratedBuffer.readShort(getAbsOffset(offset));
	}

	@Override
	public final int writeShort(long offset, short value) {
		return _decoratedBuffer.writeShort(getAbsOffset(offset), value);
	}

	@Override
	public final int readInt(long offset) {
		return _decoratedBuffer.readInt(getAbsOffset(offset));
	}

	@Override
	public final int writeInt(long offset, int value) {
		return _decoratedBuffer.writeInt(getAbsOffset(offset), value);
	}

	@Override
	public final long readLong(long offset) {
		return _decoratedBuffer.readLong(getAbsOffset(offset));
	}

	@Override
	public final int writeLong(long offset, long value) {
		return _decoratedBuffer.writeLong(getAbsOffset(offset), value);
	}

	@Override
	public final float readFloat(long offset) {
		return _decoratedBuffer.readFloat(getAbsOffset(offset));
	}

	@Override
	public final int writeFloat(long offset, float value) {
		return _decoratedBuffer.writeFloat(getAbsOffset(offset), value);
	}

	@Override
	public final double readDouble(long offset) {
		return _decoratedBuffer.readDouble(getAbsOffset(offset));
	}

	@Override
	public final int writeDouble(long offset, double value) {
		return _decoratedBuffer.writeDouble(getAbsOffset(offset), value);
	}

	@Override
	public final byte[] readByteSegment(long offset) {
		return _decoratedBuffer.readByteSegment(getAbsOffset(offset));
	}

	@Override
	public final int writeByteSegment(long offset, byte[] src, int srcOffset, int srcLength) {
		return _decoratedBuffer.writeByteSegment(getAbsOffset(offset), src, srcOffset, srcLength);
	}

	@Override
	public final int writeByteSegment(long offset, byte[] src) {
		return _decoratedBuffer.writeByteSegment(getAbsOffset(offset), src);
	}

	@Override
	public final String read8BitCharString(long offset) {
		return _decoratedBuffer.read8BitCharString(getAbsOffset(offset));
	}

	@Override
	public final int write8BitCharString(long offset, String value) {
		return _decoratedBuffer.write8BitCharString(getAbsOffset(offset), value);
	}

	@Override
	public final String readString(long offset, String charsetName) {
		return _decoratedBuffer.readString(getAbsOffset(offset), charsetName);
	}

	@Override
	public final String readString(long offset, Charset charset) {
		return _decoratedBuffer.readString(getAbsOffset(offset), charset);
	}

	@Override
	public final int writeString(long offset, String value, String charsetName) {
		return _decoratedBuffer.writeString(getAbsOffset(offset), value, charsetName);
	}

	@Override
	public final int writeString(long offset, String value, Charset charset) {
		return _decoratedBuffer.writeString(getAbsOffset(offset), value, charset);
	}

	@Override
	public final UUID readUUID(long offset) {
		return _decoratedBuffer.readUUID(getAbsOffset(offset));
	}

	@Override
	public final int writeUUID(long offset, UUID value) {
		return _decoratedBuffer.writeUUID(getAbsOffset(offset), value);
	}

	@Override
	public final ClientId readClientId(long offset) {
		return _decoratedBuffer.readClientId(getAbsOffset(offset));
	}

	@Override
	public final int writeClientId(long offset, ClientId clientId) {
		return _decoratedBuffer.writeClientId(getAbsOffset(offset), clientId);
	}

	@Override
	public final RunningStats readRunningStats(long offset) {
		return _decoratedBuffer.readRunningStats(getAbsOffset(offset));
	}

	@Override
	public final int writeRunningStats(long offset, RunningStats value) {
		return _decoratedBuffer.writeRunningStats(getAbsOffset(offset), value);
	}

	@Override
	public final String toString() {
		return String.format("Slice [%d - ] of %s", _offset, _decoratedBuffer.toString());
	}

	@Override
	public final int contentHashCode() {
		return _decoratedBuffer.contentHashCode();
	}

	@Override
	public final int contentHashCode(long offset, int length) {
		return _decoratedBuffer.contentHashCode(offset, length);
	}

	@Override
	public final boolean contentEquals(ResizingBuffer other) {
		return _decoratedBuffer.contentEquals(other);
	}

	@Override
	public final boolean contentEquals(ResizingBuffer other, long thisOffset,
			long otherOffset, int length) {
		return _decoratedBuffer.contentEquals(other, thisOffset, otherOffset, length);
	}
	
}
