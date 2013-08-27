package com.adamroughton.concentus.data;

import java.nio.charset.Charset;
import java.util.UUID;

import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.util.RunningStats;

final class NullResizingBufferSlice implements ResizingBuffer {
	
	public static final NullResizingBufferSlice INSTANCE = new NullResizingBufferSlice();
	
	@Override
	public int getContentSize() {
		return 0;
	}
	
	@Override
	public void reset() {
	}
	
	@Override
	public void reset(int offset) {
	}
	
	@Override
	public void clear(int offset) {
	}
	
	@Override
	public void clear(int offset, int length) {
	}
	
	@Override
	public ResizingBuffer slice(int offset) {
		return this;
	}
	
	@Override
	public void preallocate(int offset, int requiredSize) {
	}

	@Override
	public void copyTo(ResizingBuffer dest) {
	}

	@Override
	public void copyTo(ResizingBuffer dest, int srcOffset) {
	}

	@Override
	public void copyTo(ResizingBuffer dest, int destOffset, int length) {
	}

	@Override
	public void copyTo(ResizingBuffer dest, int srcOffset, int destOffset,
			int length) {
	}

	@Override
	public void copyTo(byte[] dest, int destOffset, int length) {
	}

	@Override
	public void copyTo(byte[] dest, int destOffset, int srcOffset, int length) {
	}

	@Override
	public void copyFrom(byte[] src, int srcOffset, int length) {
	}

	@Override
	public void copyFrom(byte[] src, int srcOffset, int destOffset, int length) {
	}
	
	@Override
	public ResizingBuffer copyOf() {
		return this;
	}

	@Override
	public ResizingBuffer copyOf(int offset, int length) {
		return this;
	}

	@Override
	public boolean readBoolean(long offset) {
		return false;
	}

	@Override
	public int writeBoolean(long offset, boolean value) {
		return 0;
	}

	@Override
	public boolean readFlagFromByte(long fieldOffset, int flagOffset) {
		return false;
	}

	@Override
	public int writeFlagToByte(long fieldOffset, int flagOffset, boolean raised) {
		return 0;
	}

	@Override
	public boolean readFlag(long fieldOffset, int fieldByteLength,
			int flagOffset) {
		return false;
	}

	@Override
	public int writeFlag(long fieldOffset, int fieldByteLength, int flagOffset,
			boolean raised) {
		return 0;
	}

	@Override
	public byte readByte(long offset) {
		return 0;
	}

	@Override
	public int writeByte(long offset, byte value) {
		return 0;
	}

	@Override
	public byte[] readBytes(long offset, int length) {
		return new byte[0];
	}

	@Override
	public int writeBytes(long offset, byte[] value) {
		return 0;
	}
	
	@Override
	public char readChar(long offset) {
		return 0;
	}

	@Override
	public int writeChar(long offset, char value) {
		return 0;
	}

	@Override
	public int read4BitUInt(long fieldOffset, int bitOffset) {
		return 0;
	}

	@Override
	public int write4BitUInt(long fieldOffset, int bitOffset, int value) {
		return 0;
	}

	@Override
	public short readShort(long offset) {
		return 0;
	}

	@Override
	public int writeShort(long offset, short value) {
		return 0;
	}

	@Override
	public int readInt(long offset) {
		return 0;
	}

	@Override
	public int writeInt(long offset, int value) {
		return 0;
	}

	@Override
	public long readLong(long offset) {
		return 0;
	}

	@Override
	public int writeLong(long offset, long value) {
		return 0;
	}

	@Override
	public float readFloat(long offset) {
		return 0;
	}

	@Override
	public int writeFloat(long offset, float value) {
		return 0;
	}

	@Override
	public double readDouble(long offset) {
		return 0;
	}

	@Override
	public int writeDouble(long offset, double value) {
		return 0;
	}

	@Override
	public byte[] readByteSegment(long offset) {
		return new byte[0];
	}

	@Override
	public int writeByteSegment(long offset, byte[] src, int srcOffset, int srcLength) {
		return 0;
	}

	@Override
	public int writeByteSegment(long offset, byte[] src) {
		return 0;
	}

	@Override
	public String read8BitCharString(long offset) {
		return "";
	}

	@Override
	public int write8BitCharString(long offset, String value) {
		return 0;
	}

	@Override
	public String readString(long offset, String charsetName) {
		return "";
	}

	@Override
	public String readString(long offset, Charset charset) {
		return "";
	}

	@Override
	public int writeString(long offset, String value, String charsetName) {
		return 0;
	}

	@Override
	public int writeString(long offset, String value, Charset charset) {
		return 0;
	}

	@Override
	public UUID readUUID(long offset) {
		return new UUID(0, 0);
	}

	@Override
	public int writeUUID(long offset, UUID value) {
		return 0;
	}

	@Override
	public ClientId readClientId(long offset) {
		return new ClientId(0, 0);
	}

	@Override
	public int writeClientId(long offset, ClientId clientId) {
		return 0;
	}

	@Override
	public RunningStats readRunningStats(long offset) {
		return new RunningStats();
	}

	@Override
	public int writeRunningStats(long offset, RunningStats value) {
		return 0;
	}

	@Override
	public String toString() {
		return "NullResizingBuffer";
	}

	@Override
	public int contentHashCode() {
		return super.hashCode();
	}

	@Override
	public int contentHashCode(long offset, int length) {
		return super.hashCode();
	}

	@Override
	public boolean contentEquals(ResizingBuffer other) {
		return other == this;
	}

	@Override
	public boolean contentEquals(ResizingBuffer other, long thisOffset,
			long otherOffset, int length) {
		return other == this;
	}
	
}
