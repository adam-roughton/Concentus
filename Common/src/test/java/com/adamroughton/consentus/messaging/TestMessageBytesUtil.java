package com.adamroughton.consentus.messaging;

import java.nio.ByteBuffer;

import org.junit.*;
import static org.junit.Assert.*;

public class TestMessageBytesUtil {

	@Test
	public void testReadChar() {
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.putChar('a');
		assertEquals('a', MessageBytesUtil.readChar(bb.array(), 0));
	}
	
	@Test
	public void testReadChar_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(6);
		bb.putChar(1, 'A');
		assertEquals('A', MessageBytesUtil.readChar(bb.array(), 1));
	}
	
	@Test
	public void testWriteChar() {
		byte[] array = new byte[2];
		MessageBytesUtil.writeChar(array, 0, '\u2202');
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals('\u2202', bb.getChar());
	}
	
	@Test
	public void testWriteChar_NonZeroOffset() {
		byte[] array = new byte[6];
		MessageBytesUtil.writeInt(array, 1, '\u2202');
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals('\u2202', bb.getInt(1));
	}
	
	@Test
	public void testReadShort() {
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.putShort((short)25);
		assertEquals(25, MessageBytesUtil.readShort(bb.array(), 0));
	}
	
	@Test
	public void testReadShort_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(6);
		bb.putShort(1, (short)25);
		assertEquals(25, MessageBytesUtil.readShort(bb.array(), 1));
	}
	
	@Test
	public void testWriteShort() {
		byte[] array = new byte[2];
		MessageBytesUtil.writeShort(array, 0, (short)25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getShort());
	}
	
	@Test
	public void testWriteShort_NonZeroOffset() {
		byte[] array = new byte[6];
		MessageBytesUtil.writeShort(array, 1, (short)25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getShort(1));
	}
	
	@Test
	public void testReadInt() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(25);
		assertEquals(25, MessageBytesUtil.readInt(bb.array(), 0));
	}
	
	@Test
	public void testReadInt_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putInt(1, 25);
		assertEquals(25, MessageBytesUtil.readInt(bb.array(), 1));
	}
	
	@Test
	public void testWriteInt() {
		byte[] array = new byte[4];
		MessageBytesUtil.writeInt(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getInt());
	}
	
	@Test
	public void testWriteInt_NonZeroOffset() {
		byte[] array = new byte[12];
		MessageBytesUtil.writeInt(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getInt(1));
	}
	
	@Test
	public void testReadLong() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(25);
		assertEquals(25, MessageBytesUtil.readLong(bb.array(), 0));
	}
	
	@Test
	public void testReadLong_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(24);
		bb.putLong(1, 25);
		assertEquals(25, MessageBytesUtil.readLong(bb.array(), 1));
	}
	
	@Test
	public void testWriteLong() {
		byte[] array = new byte[8];
		MessageBytesUtil.writeLong(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getLong());
	}
	
	@Test
	public void testWriteLong_NonZeroOffset() {
		byte[] array = new byte[24];
		MessageBytesUtil.writeLong(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getLong(1));
	}
	
	@Test
	public void testReadFloat() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putFloat(25);
		assertEquals(25, MessageBytesUtil.readFloat(bb.array(), 0), 0);
	}
	
	@Test
	public void testReadFloat_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putFloat(1, 25);
		assertEquals(25, MessageBytesUtil.readFloat(bb.array(), 1), 0);
	}
	
	@Test
	public void testWriteFloat() {
		byte[] array = new byte[4];
		MessageBytesUtil.writeFloat(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getFloat(), 0);
	}
	
	@Test
	public void testWriteFloat_NonZeroOffset() {
		byte[] array = new byte[12];
		MessageBytesUtil.writeFloat(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getFloat(1), 0);
	}
	
	@Test
	public void testReadDouble() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(25);
		assertEquals(25, MessageBytesUtil.readDouble(bb.array(), 0), 0);
	}
	
	@Test
	public void testReadDouble_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(24);
		bb.putDouble(1, 25);
		assertEquals(25, MessageBytesUtil.readDouble(bb.array(), 1), 0);
	}
	
	@Test
	public void testWriteDouble() {
		byte[] array = new byte[8];
		MessageBytesUtil.writeDouble(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getDouble(), 0);
	}
	
	@Test
	public void testWriteDouble_NonZeroOffset() {
		byte[] array = new byte[24];
		MessageBytesUtil.writeDouble(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getDouble(1), 0);
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_AllRaised() {
		byte[] array = new byte[1];
		array[0] = (byte) 0xff;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 0, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}	
	
	@Test
	public void testReadFlagSingleByteFlagField_AllRaised_NonZeroOffset() {
		byte[] array = new byte[3];
		array[1] = (byte) 0xff;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 1, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_5thFlagRaised() {
		byte[] array = new byte[1];
		array[0] = (byte) 0x8;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 0, i);
			if (i == 4) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_5thFlagRaised_NonZeroOffset() {
		byte[] array = new byte[3];
		array[1] = (byte) 0x8;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 1, i);
			if (i == 4) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_1stFlagRaised() {
		byte[] array = new byte[1];
		array[0] = (byte) 0x80;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 0, i);
			if (i == 0) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_1stFlagRaised_NonZeroOffset() {
		byte[] array = new byte[3];
		array[1] = (byte) 0x80;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 1, i);
			if (i == 0) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_LastFlagRaised() {
		byte[] array = new byte[1];
		array[0] = (byte) 0x1;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 0, i);
			if (i == 7) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_LastFlagRaised_NonZeroOffset() {
		byte[] array = new byte[3];
		array[1] = (byte) 0x1;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlagFromByte(array, 1, i);
			if (i == 7) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_RaiseAll() {
		byte[] array = new byte[1];
		for (int i = 0; i < 8; i++) {
			MessageBytesUtil.writeFlagToByte(array, 0, i, true);
		}
		assertEquals(0xFF, 0xFF & array[0]);
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_Raise3rd() {
		byte[] array = new byte[1];
		MessageBytesUtil.writeFlagToByte(array, 0, 2, true);
		assertEquals(0x20, 0xFF & array[0]);
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_Raise1st() {
		byte[] array = new byte[1];
		MessageBytesUtil.writeFlagToByte(array, 0, 0, true);
		assertEquals(0x80, 0xFF & array[0]);
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_RaiseLast() {
		byte[] array = new byte[1];
		MessageBytesUtil.writeFlagToByte(array, 0, 7, true);
		assertEquals(0x1, 0xFF & array[0]);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testReadFlag_FlagOutOfRange() {
		MessageBytesUtil.readFlag(new byte[2], 0, 2, 16);
	}
	
	@Test
	public void testReadFlag_AllRaised() {
		byte[] array = new byte[16];
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) 0xff;
		}
		for (int i = 0; i < 128; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 0, 16, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}
	
	@Test
	public void testReadFlag_AllRaised_NonZeroOffset() {
		byte[] array = new byte[22];
		for (int i = 6; i < array.length; i++) {
			array[i] = (byte) 0xff;
		}
		for (int i = 0; i < 128; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 6, 16, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}
	
	@Test
	public void testReadFlag_102ndFlagRaised() {
		byte[] array = new byte[16];
		array[12] = (byte) 0x4;
		for (int i = 0; i < 128; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 0, 16, i);
			if (i == 101) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlag_102ndFlagRaised_NonZeroOffset() {
		byte[] array = new byte[22];
		array[18] = (byte) 0x4;
		for (int i = 0; i < 128; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 6, 16, i);
			if (i == 101) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlag_1stFlagRaised() {
		byte[] array = new byte[16];
		array[0] = (byte) 0x80;
		for (int i = 0; i < 128; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 0, 16, i);
			if (i == 0) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlag_1stFlagRaised_NonZeroOffset() {
		byte[] array = new byte[22];
		array[6] = (byte) 0x80;
		for (int i = 0; i < 128; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 6, 16, i);
			if (i == 0) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlag_LastFlagRaised() {
		byte[] array = new byte[16];
		array[15] = (byte) 0x1;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 0, 16, i);
			if (i == 127) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testReadFlag_LastFlagRaised_NonZeroOffset() {
		byte[] array = new byte[22];
		array[21] = (byte) 0x1;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = MessageBytesUtil.readFlag(array, 6, 16, i);
			if (i == 127) {
				assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
			} else {
				assertFalse(String.format("Flag index %d = %b", i, flagVal), flagVal);
			}
		}
	}
	
	@Test
	public void testWriteFlag_RaiseAll() {
		byte[] array = new byte[16];
		for (int i = 0; i < 128; i++) {
			MessageBytesUtil.writeFlag(array, 0, 16, i, true);
		}
		for (int i = 0; i < array.length; i++) {
			assertEquals(String.format("Byte Index %d", i), 0xFF, 0xFF & array[i]);
		}
	}
	
	private void assertLoneFlagIsRaised(int expectedFlagIndex, byte[] array) {
		int flagByte = expectedFlagIndex / 8;
		// MSB is index 0
		int expectedFlagByteVal =  1 << (7 - (expectedFlagIndex % 8));
		
		int expected;
		for (int i = 0; i < array.length; i++) {
			if (i == flagByte) {
				expected = expectedFlagByteVal;
			} else {
				expected = 0;
			}
			assertEquals(String.format("Byte Index %d", i), expected, 0xFF & array[i]);
		}
	}
	
	@Test
	public void testWriteFlag_Raise74th() {
		byte[] array = new byte[16];
		MessageBytesUtil.writeFlag(array, 0, 16, 73, true);
		assertLoneFlagIsRaised(73, array);
	}
	
	@Test
	public void testWriteFlag_Raise1st() {
		byte[] array = new byte[16];
		MessageBytesUtil.writeFlag(array, 0, 16, 0, true);
		assertLoneFlagIsRaised(0, array);
	}
	
	@Test
	public void testWriteFlag_RaiseLast() {
		byte[] array = new byte[16];
		MessageBytesUtil.writeFlag(array, 0, 16, 127, true);
		assertLoneFlagIsRaised(127, array);
	}
	
	
}
