package com.adamroughton.concentus.data;

import java.nio.charset.Charset;
import java.util.UUID;

import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.util.RunningStats;

public interface ResizingBuffer {

	final int INT_SIZE = Integer.SIZE / 8;
	final int SHORT_SIZE = Short.SIZE / 8;
	final int LONG_SIZE = Long.SIZE / 8;
	final int CHAR_SIZE = Character.SIZE / 8;
	final int BOOL_SIZE = 1;
	final int FLOAT_SIZE = Float.SIZE / 8;
	final int DOUBLE_SIZE = Double.SIZE / 8;
	final int UUID_SIZE = 16;
	final int CLIENT_ID_SIZE = BytesUtil.CLIENT_ID_SIZE;
	final int RUNNING_STATS_SIZE = BytesUtil.RUNNING_STATS_SIZE;
	
	/**
	 * Gets the size of the content currently stored in this buffer.
	 * @return
	 */
	int getContentSize();
	
	void reset();
	
	void reset(int offset);
	
	void clear(int offset);
	
	void clear(int offset, int length);
	
	ResizingBuffer slice(int offset);
	
	void preallocate(int offset, int requiredSize);
	
	void copyTo(ResizingBuffer dest);
	
	void copyTo(ResizingBuffer dest, int srcOffset);
	
	void copyTo(ResizingBuffer dest, int destOffset, int length);
	
	void copyTo(ResizingBuffer dest, int srcOffset, int destOffset, int length);
	
	void copyTo(byte[] dest, int destOffset, int length);
	
	void copyTo(byte[] dest, int destOffset, int srcOffset, int length);
	
	void copyFrom(byte[] src, int srcOffset, int length);
	
	void copyFrom(byte[] src, int srcOffset, int destOffset, int length);
	
	ResizingBuffer copyOf();
	
	ResizingBuffer copyOf(int offset, int length);
	
	boolean readBoolean(long offset);
	
	int writeBoolean(long offset, boolean value);
	
	boolean readFlagFromByte(long fieldOffset, int flagOffset);
	
	int writeFlagToByte(long fieldOffset, int flagOffset, boolean raised);
	
	boolean readFlag(long fieldOffset, int fieldByteLength, int flagOffset);
	
	int writeFlag(long fieldOffset, int fieldByteLength, int flagOffset, boolean raised);
	
	byte readByte(long offset);
	
	int writeByte(long offset, byte value);
	
	byte[] readBytes(long offset, int length);
	
	int writeBytes(long offset, byte[] value);
	
	char readChar(long offset);
	
	int writeChar(long offset, char value);
	
	int read4BitUInt(long fieldOffset, int bitOffset);
	
	int write4BitUInt(long fieldOffset, int bitOffset, int value);
	
	short readShort(long offset);
	
	int writeShort(long offset, short value);
	
	int readInt(long offset);
	
	int writeInt(long offset, int value);
	
	long readLong(long offset);
	
	int writeLong(long offset, long value);
	
	float readFloat(long offset);
	
	int writeFloat(long offset, float value);
	
	double readDouble(long offset);
	
	int writeDouble(long offset, double value);
	
	/**
	 * Reads a length encoded segments of bytes from the buffer, using the integer
	 * starting at {@code offset} for the segment length.
	 * @param offset
	 * @return
	 */
	byte[] readByteSegment(long offset);
	
	int writeByteSegment(long offset, byte[] src, int srcOffset, int srcLength);
	
	int writeByteSegment(long offset, byte[] src);
	
	String read8BitCharString(long offset);
	
	int write8BitCharString(long offset, String value);
	
	String readString(long offset, String charsetName);
	
	String readString(long offset, Charset charset);
	
	int writeString(long offset, String value, String charsetName);
	
	int writeString(long offset, String value, Charset charset);
	
	UUID readUUID(long offset);
	
	int writeUUID(long offset, UUID value);
	
	ClientId readClientId(long offset);
	
	int writeClientId(long offset, final ClientId clientId);
	
	RunningStats readRunningStats(long offset);
	
	int writeRunningStats(long offset, RunningStats value);
	
	int contentHashCode();
	
	int contentHashCode(long offset, int length);
	
	boolean contentEquals(ResizingBuffer other);
	
	boolean contentEquals(ResizingBuffer other, long thisOffset, long otherOffset, int length);
}
