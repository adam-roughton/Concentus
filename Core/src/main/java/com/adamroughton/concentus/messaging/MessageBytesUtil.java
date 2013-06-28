/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.concentus.messaging;

import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import com.adamroughton.concentus.model.ClientId;
import com.adamroughton.concentus.util.RunningStats;

import sun.misc.Unsafe;

/**
 * Allows fast reading and writing of primitives and primitive arrays
 * in network byte order.
 * 
 * @author Adam Roughton
 *
 */
public final class MessageBytesUtil {

	private static final Unsafe _unsafe;
	private static final boolean _isLittleEndian;
	static {
		try {
			Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			_unsafe = (Unsafe) field.get(null);
			
			// Endianness check
			long[] l = new long[1];
			l[0] = 25;
			_isLittleEndian = 25 == _unsafe.getLong(l, (long)_unsafe.arrayBaseOffset(long[].class));
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private static final int BYTE_ARRAY_OFFSET = _unsafe.arrayBaseOffset(byte[].class);
	//private static final int INT_ARRAY_OFFSET = _unsafe.arrayBaseOffset(int[].class);
	//private static final int LONG_ARRAY_OFFSET = _unsafe.arrayBaseOffset(long[].class);
	
	public static boolean readBoolean(byte[] array, long offset) {
		return _unsafe.getBoolean(array, BYTE_ARRAY_OFFSET + offset);
	}
	
	public static void writeBoolean(byte[] array, long offset, boolean value) {
		_unsafe.putBoolean(array, BYTE_ARRAY_OFFSET + offset, value);
	}
	
	private static int getFlagMask(int flagOffset) {
		return 1 << (_isLittleEndian? 7 - flagOffset : flagOffset);
	}
	
	public static boolean readFlagFromByte(byte[] array, long fieldOffset, int flagOffset) {	
		int flagMask = getFlagMask(flagOffset);
		byte field = _unsafe.getByte(array, BYTE_ARRAY_OFFSET + fieldOffset);
		return (field & flagMask) != 0;
	}
	
	public static void writeFlagToByte(byte[] array, long fieldOffset, int flagOffset, boolean raised) {
		int flagMask = getFlagMask(flagOffset);
		byte field = _unsafe.getByte(array, BYTE_ARRAY_OFFSET + fieldOffset);
		field = (byte) (raised? field | flagMask : field & ~flagMask);
		_unsafe.putByte(array, BYTE_ARRAY_OFFSET + fieldOffset, field);
	}
	
	private static long getFieldOffset(long fieldOffset, int flagOffset) {
		return (flagOffset / 8) + fieldOffset;
	}
	
	public static boolean readFlag(byte[] array, long fieldOffset, int fieldByteLength, int flagOffset) {
		long fieldOffsetForFlag = getFieldOffset(fieldOffset, flagOffset);	
		return readFlagFromByte(array, fieldOffsetForFlag, flagOffset % 8);
	}
	
	public static void writeFlag(byte[] array, long fieldOffset, int fieldByteLength, int flagOffset, boolean raised) {
		long fieldOffsetForFlag = getFieldOffset(fieldOffset, flagOffset);	
		writeFlagToByte(array, fieldOffsetForFlag, flagOffset % 8, raised);
	}
	
	public static char readChar(byte[] array, long offset) {
		char res = _unsafe.getChar(array, BYTE_ARRAY_OFFSET + offset);
		return _isLittleEndian? Character.reverseBytes(res) : res;
	}
	
	public static void writeChar(byte[] array, long offset, char value) {
		value = _isLittleEndian? Character.reverseBytes(value) : value;
		_unsafe.putChar(array, BYTE_ARRAY_OFFSET + offset, value);
	}
	
	private static int get4BitUIntMask(int bitOffset) {
		return 0xF << (_isLittleEndian? 4 - bitOffset : bitOffset);
	} 
	
	public static int read4BitUInt(byte[] array, long fieldOffset, int bitOffset) {
		int intMask = get4BitUIntMask(bitOffset);
		byte field = _unsafe.getByte(array, BYTE_ARRAY_OFFSET + fieldOffset);
		int val = (field & intMask);
		val >>= (_isLittleEndian? 4 - bitOffset : bitOffset);
		return val;
	}
	
	public static void write4BitUInt(byte[] array, long fieldOffset, int bitOffset, int value) {
		int intMask = get4BitUIntMask(bitOffset);
		byte field = _unsafe.getByte(array, BYTE_ARRAY_OFFSET + fieldOffset);
		int val = (value & 0xF) << (_isLittleEndian? 4 - bitOffset : bitOffset);
		field = (byte) ((field & ~intMask) | val);
		_unsafe.putByte(array, BYTE_ARRAY_OFFSET + fieldOffset, field);
	}
	
	public static short readShort(byte[] array, long offset) {
		short res = _unsafe.getShort(array, BYTE_ARRAY_OFFSET + offset);
		return _isLittleEndian? Short.reverseBytes(res) : res;
	}
	
	public static void writeShort(byte[] array, long offset, short value) {
		value = _isLittleEndian? Short.reverseBytes(value) : value;
		_unsafe.putShort(array, BYTE_ARRAY_OFFSET + offset, value);
	}
	
	public static int readInt(byte[] array, long offset) {
		int res = _unsafe.getInt(array, BYTE_ARRAY_OFFSET + offset);
		return _isLittleEndian? Integer.reverseBytes(res) : res;
	}
	
	public static void writeInt(byte[] array, long offset, int value) {
		value = _isLittleEndian? Integer.reverseBytes(value) : value;
		_unsafe.putInt(array, BYTE_ARRAY_OFFSET + offset, value);
	}
	
	public static long readLong(byte[] array, long offset) {
		long res = _unsafe.getLong(array, BYTE_ARRAY_OFFSET + offset);
		return _isLittleEndian? Long.reverseBytes(res) : res;
	}
	
	public static void writeLong(byte[] array, long offset, long value) {
		value = _isLittleEndian? Long.reverseBytes(value) : value;
		_unsafe.putLong(array, BYTE_ARRAY_OFFSET + offset, value);
	}
	
	public static float readFloat(byte[] array, long offset) {
		int res = _unsafe.getInt(array, BYTE_ARRAY_OFFSET + offset);
		return Float.intBitsToFloat(_isLittleEndian? Integer.reverseBytes(res) : res);
	}
	
	public static void writeFloat(byte[] array, long offset, float value) {
		int iVal = Float.floatToRawIntBits(value);
		iVal = _isLittleEndian? Integer.reverseBytes(iVal) : iVal;
		_unsafe.putInt(array, BYTE_ARRAY_OFFSET + offset, iVal);
	}
	
	public static double readDouble(byte[] array, long offset) {
		long res = _unsafe.getLong(array, BYTE_ARRAY_OFFSET + offset);
		return Double.longBitsToDouble(_isLittleEndian? Long.reverseBytes(res) : res);
	}
	
	public static void writeDouble(byte[] array, long offset, double value) {
		long lVal = Double.doubleToRawLongBits(value);
		lVal = _isLittleEndian? Long.reverseBytes(lVal) : lVal;
		_unsafe.putLong(array, BYTE_ARRAY_OFFSET + offset, lVal);
	}
	
	public static String read8BitCharString(byte[] array, long offset) {
		return readString(array, offset, StandardCharsets.ISO_8859_1);
	}
	
	public static int write8BitCharString(byte[] array, long offset, String value) {
		return writeString(array, offset, value, StandardCharsets.ISO_8859_1);
	}
	
	public static String readString(byte[] array, long offset, String charsetName) {
		return readString(array, offset, Charset.forName(charsetName));
	}
	
	public static String readString(byte[] array, long offset, Charset charset) {
		int len = readInt(array, offset);
		byte[] stringBytes = new byte[len];
		_unsafe.copyMemory(array, BYTE_ARRAY_OFFSET + offset + 4, stringBytes, BYTE_ARRAY_OFFSET, len);
		return new String(stringBytes, charset);
	}
	
	public static int writeString(byte[] array, long offset, String value, String charsetName) {
		return writeString(array, offset, value, Charset.forName(charsetName));
	}
	
	public static int writeString(byte[] array, long offset, String value, Charset charset) {
		byte[] stringBytes = value.getBytes(charset);
		int len = stringBytes.length;
		writeInt(array, offset, len);
		_unsafe.copyMemory(stringBytes, BYTE_ARRAY_OFFSET, array, BYTE_ARRAY_OFFSET + offset + 4, len);
		return len;
	}
	
	public static UUID readUUID(byte[] array, long offset) {
		long msb = readLong(array, offset);
		long lsb = readLong(array, offset + 8);
		return new UUID(msb, lsb);
	}
	
	public static void writeUUID(byte[] array, long offset, UUID value) {
		long msb = value.getMostSignificantBits();
		long lsb = value.getLeastSignificantBits();
		writeLong(array, offset, msb);
		writeLong(array, offset + 8, lsb);
	}
	
	private static final long CLIENT_ID_NAMESPACE_MASK = (0xFFFFL << 48);
	
	public static ClientId readClientId(byte[] array, long offset) {
		long clientIdBits = readLong(array, offset);
		int namespaceId = (int)((clientIdBits & CLIENT_ID_NAMESPACE_MASK) >>> 48);
		long clientId = clientIdBits & (~CLIENT_ID_NAMESPACE_MASK);
		return new ClientId(namespaceId, clientId);
	}
	
	public static void writeClientId(byte[] array, long offset, final ClientId clientId) {
		long clientIdBits = clientId.getNamespaceId();
		clientIdBits <<= 48;
		clientIdBits &= CLIENT_ID_NAMESPACE_MASK;
		clientIdBits |= (clientId.getClientId() & (~CLIENT_ID_NAMESPACE_MASK));
		writeLong(array, offset, clientIdBits);
	}
	
	public static RunningStats readRunningStats(byte[] array, long offset) {
		int count = readInt(array, offset);
		offset += 4;
		double mean = readDouble(array, offset);
		offset += 8;
		double sumSqrs = readDouble(array, offset);
		offset += 8;
		double min = readDouble(array, offset);
		offset += 8;
		double max = readDouble(array, offset);
		
		return new RunningStats(count, mean, sumSqrs, min, max);
	}
	
	public static int writeRunningStats(byte[] array, long offset, RunningStats value) {
		writeInt(array, offset, value.getCount());
		offset += 4;
		writeDouble(array, offset, value.getMean());
		offset += 8;
		writeDouble(array, offset, value.getSumOfSquares());
		offset += 8;
		writeDouble(array, offset, value.getMin());
		offset += 8;
		writeDouble(array, offset, value.getMax());
		return 28;
	}
	
	/*public static int[] readIntArray(byte[] array, long offset) {
		int arraySize = readInt(array, offset);
		if (arraySize >= array.length >> 2) // sanity check
			throw new RuntimeException(String.format("Int array size was %d, " +
					"bigger than array capacity (length = %d).", arraySize, array.length));
		int[] res = new int[arraySize];
		
	}*/
	
	public static void clear(byte[] event, int offset, int length) {
		int lastHeaderByte = offset + length - 1;
		while (offset + 7 <= lastHeaderByte) {
			MessageBytesUtil.writeLong(event, offset, 0);
			offset += 8;
		}
		while (offset + 3 <= lastHeaderByte) {
			MessageBytesUtil.writeInt(event, offset, 0);
			offset += 4;
		}
		while (offset <= lastHeaderByte) {
			event[offset++] = 0;
		}
	}
	
}
