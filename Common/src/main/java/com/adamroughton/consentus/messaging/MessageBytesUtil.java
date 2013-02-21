package com.adamroughton.consentus.messaging;

import java.lang.reflect.Field;

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
	
	private static long getFieldOffset(long fieldOffset, int fieldByteLength, int flagOffset) {
		long fieldOffsetForFlag = (flagOffset / 8) + fieldOffset;
		if (fieldOffsetForFlag >= fieldOffset + fieldByteLength)
			throw new IllegalArgumentException(String.format("The required field offset for flag %d is %d, " +
					"which is outside the bounds of the field {%d, %d}.", 
					flagOffset, fieldOffsetForFlag, fieldOffset, fieldOffset + fieldByteLength - 1));
		return fieldOffsetForFlag;
	}
	
	public static boolean readFlag(byte[] array, long fieldOffset, int fieldByteLength, int flagOffset) {
		long fieldOffsetForFlag = getFieldOffset(fieldOffset, fieldByteLength, flagOffset);	
		return readFlagFromByte(array, fieldOffsetForFlag, flagOffset % 8);
	}
	
	public static void writeFlag(byte[] array, long fieldOffset, int fieldByteLength, int flagOffset, boolean raised) {
		long fieldOffsetForFlag = getFieldOffset(fieldOffset, fieldByteLength, flagOffset);	
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
	
	/*public static int[] readIntArray(byte[] array, long offset) {
		int arraySize = readInt(array, offset);
		if (arraySize >= array.length >> 2) // sanity check
			throw new RuntimeException(String.format("Int array size was %d, " +
					"bigger than array capacity (length = %d).", arraySize, array.length));
		int[] res = new int[arraySize];
		
	}*/
	
}
