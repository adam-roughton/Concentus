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
package com.adamroughton.concentus.data;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.*;

import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.data.model.ClientId;

import static org.junit.Assert.*;

public class TestBytesUtil {

	@Test
	public void testReadChar() {
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.putChar('a');
		assertEquals('a', BytesUtil.readChar(bb.array(), 0));
	}
	
	@Test
	public void testReadChar_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(6);
		bb.putChar(1, 'A');
		assertEquals('A', BytesUtil.readChar(bb.array(), 1));
	}
	
	@Test
	public void testWriteChar() {
		byte[] array = new byte[2];
		BytesUtil.writeChar(array, 0, '\u2202');
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals('\u2202', bb.getChar());
	}
	
	@Test
	public void testWriteChar_NonZeroOffset() {
		byte[] array = new byte[6];
		BytesUtil.writeInt(array, 1, '\u2202');
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals('\u2202', bb.getInt(1));
	}
	
	@Test
	public void testRead4BitUInt() {
		byte[] array = new byte[1];
		int expected = 11;
		array[0] = (byte) ((0xFF & expected) << 4);
		assertEquals(expected, BytesUtil.read4BitUInt(array, 0, 0));
	}
	
	@Test
	public void testRead4BitUInt_NonZeroOffset() {
		byte[] array = new byte[4];
		int expected = 11;
		array[3] = (byte) ((0xFF & expected) << 4);
		assertEquals(expected, BytesUtil.read4BitUInt(array, 3, 0));
	}
	
	@Test
	public void testWrite4BitUInt() {
		byte[] array = new byte[1];
		int expected = 11;
		BytesUtil.write4BitUInt(array, 0, 0, expected);
		byte expByte = (byte) ((0xFF & expected) << 4);
		assertEquals(expByte, array[0]);
	}
	
	@Test
	public void testWrite4BitUInt_NonZeroOffset() {
		byte[] array = new byte[5];
		int expected = 11;
		BytesUtil.write4BitUInt(array, 3, 0, expected);
		byte expByte = (byte) ((0xFF & expected) << 4);
		assertEquals(expByte, array[3]);
	}
	
	@Test
	public void testRead4BitUInt_BitOffset2() {
		byte[] array = new byte[1];
		int expected = 11;
		array[0] = (byte) ((0xFF & expected) << 2);
		assertEquals(expected, BytesUtil.read4BitUInt(array, 0, 2));
	}
	
	@Test
	public void testWrite4BitUInt_BitOffset2() {
		byte[] array = new byte[1];
		int expected = 11;
		BytesUtil.write4BitUInt(array, 0, 2, expected);
		byte expByte = (byte) ((0xFF & expected) << 2);
		assertEquals(expByte, array[0]);
	}
	
	@Test
	public void testRead4BitUInt_BitOffset4() {
		byte[] array = new byte[1];
		int expected = 11;
		array[0] = (byte) (0xFF & expected);
		assertEquals(expected, BytesUtil.read4BitUInt(array, 0, 4));
	}
	
	@Test
	public void testWrite4BitUInt_BitOffset4() {
		byte[] array = new byte[1];
		int expected = 11;
		BytesUtil.write4BitUInt(array, 0, 4, expected);
		byte expByte = (byte) (0xFF & expected);
		assertEquals(expByte, array[0]);
	}
	
	@Test
	public void testRead4BitUInt_BitOffset4WithOtherFlag() {
		byte[] array = new byte[1];
		int expected = 11;
		BytesUtil.writeFlagToByte(array, 0, 1, true);
		array[0] = (byte) ((0xFF & expected) << 2);
		assertEquals(expected, BytesUtil.read4BitUInt(array, 0, 2));
	}
	
	@Test
	public void testWrite4BitUInt_BitOffset4WithOtherFlag() {
		byte[] array = new byte[1];
		int expected = 11;
		BytesUtil.writeFlagToByte(array, 0, 1, true);
		BytesUtil.write4BitUInt(array, 0, 2, expected);
		byte expByte = (byte) (((0xFF & expected) << 2) | 0x40);
		assertEquals(expByte, array[0]);
		assertTrue(BytesUtil.readFlagFromByte(array, 0, 1));
		assertEquals(expected, BytesUtil.read4BitUInt(array, 0, 2));
	}
	
	@Test
	public void testReadShort() {
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.putShort((short)25);
		assertEquals(25, BytesUtil.readShort(bb.array(), 0));
	}
	
	@Test
	public void testReadShort_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(6);
		bb.putShort(1, (short)25);
		assertEquals(25, BytesUtil.readShort(bb.array(), 1));
	}
	
	@Test
	public void testWriteShort() {
		byte[] array = new byte[2];
		BytesUtil.writeShort(array, 0, (short)25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getShort());
	}
	
	@Test
	public void testWriteShort_NonZeroOffset() {
		byte[] array = new byte[6];
		BytesUtil.writeShort(array, 1, (short)25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getShort(1));
	}
	
	@Test
	public void testReadInt() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(25);
		assertEquals(25, BytesUtil.readInt(bb.array(), 0));
	}
	
	@Test
	public void testReadInt_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putInt(1, 25);
		assertEquals(25, BytesUtil.readInt(bb.array(), 1));
	}
	
	@Test
	public void testWriteInt() {
		byte[] array = new byte[4];
		BytesUtil.writeInt(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getInt());
	}
	
	@Test
	public void testWriteInt_NonZeroOffset() {
		byte[] array = new byte[12];
		BytesUtil.writeInt(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getInt(1));
	}
	
	@Test
	public void testReadLong() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(25);
		assertEquals(25, BytesUtil.readLong(bb.array(), 0));
	}
	
	@Test
	public void testReadLong_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(24);
		bb.putLong(1, 25);
		assertEquals(25, BytesUtil.readLong(bb.array(), 1));
	}
	
	@Test
	public void testWriteLong() {
		byte[] array = new byte[8];
		BytesUtil.writeLong(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getLong());
	}
	
	@Test
	public void testWriteLong_NonZeroOffset() {
		byte[] array = new byte[24];
		BytesUtil.writeLong(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getLong(1));
	}
	
	@Test
	public void testReadFloat() {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putFloat(25);
		assertEquals(25, BytesUtil.readFloat(bb.array(), 0), 0);
	}
	
	@Test
	public void testReadFloat_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putFloat(1, 25);
		assertEquals(25, BytesUtil.readFloat(bb.array(), 1), 0);
	}
	
	@Test
	public void testWriteFloat() {
		byte[] array = new byte[4];
		BytesUtil.writeFloat(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getFloat(), 0);
	}
	
	@Test
	public void testWriteFloat_NonZeroOffset() {
		byte[] array = new byte[12];
		BytesUtil.writeFloat(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getFloat(1), 0);
	}
	
	@Test
	public void testReadDouble() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putDouble(25);
		assertEquals(25, BytesUtil.readDouble(bb.array(), 0), 0);
	}
	
	@Test
	public void testReadDouble_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(24);
		bb.putDouble(1, 25);
		assertEquals(25, BytesUtil.readDouble(bb.array(), 1), 0);
	}
	
	@Test
	public void testWriteDouble() {
		byte[] array = new byte[8];
		BytesUtil.writeDouble(array, 0, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getDouble(), 0);
	}
	
	@Test
	public void testWriteDouble_NonZeroOffset() {
		byte[] array = new byte[24];
		BytesUtil.writeDouble(array, 1, 25);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(25, bb.getDouble(1), 0);
	}
	
	@Test
	public void testReadUUID() {
		ByteBuffer bb = ByteBuffer.allocate(16);
		
		UUID expected = UUID.fromString("abababab-abab-abab-abab-abababababab");
		long msb = expected.getMostSignificantBits();
		long lsb = expected.getLeastSignificantBits();
		bb.putLong(msb);
		bb.putLong(lsb);
		assertEquals(expected, BytesUtil.readUUID(bb.array(), 0));
	}
	
	@Test
	public void testReadUUID_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(32);
		
		UUID expected = UUID.fromString("abababab-abab-abab-abab-abababababab");
		long msb = expected.getMostSignificantBits();
		long lsb = expected.getLeastSignificantBits();
		bb.putLong(1, msb);
		bb.putLong(9, lsb);
		assertEquals(expected, BytesUtil.readUUID(bb.array(), 1));
	}
	
	@Test
	public void testWriteUUID() {
		byte[] array = new byte[16];
		UUID expected = UUID.fromString("abababab-abab-abab-abab-abababababab");
		
		BytesUtil.writeUUID(array, 0, expected);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(expected.getMostSignificantBits(), bb.getLong());
		assertEquals(expected.getLeastSignificantBits(), bb.getLong());
	}
	
	@Test
	public void testWriteUUID_NonZeroOffset() {
		byte[] array = new byte[32];
		UUID expected = UUID.fromString("abababab-abab-abab-abab-abababababab");
		
		BytesUtil.writeUUID(array, 1, expected);
		ByteBuffer bb = ByteBuffer.wrap(array);
		assertEquals(expected.getMostSignificantBits(), bb.getLong(1));
		assertEquals(expected.getLeastSignificantBits(), bb.getLong(9));
	}
	
	@Test
	public void testReadClientId() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		
		ClientId expected = new ClientId(101, 888);
		long clientIdBits = 0x0065000000000378L;
		bb.putLong(clientIdBits);
		
		ClientId actual = BytesUtil.readClientId(bb.array(), 0);
		assertEquals(expected.getNamespaceId(), actual.getNamespaceId());
		assertEquals(expected.getClientIndex(), actual.getClientIndex());
	}
	
	@Test
	public void testReadMaxClientId() {
		ByteBuffer bb = ByteBuffer.allocate(8);
		
		ClientId expected = new ClientId(65535, 281474976710655L);
		long clientIdBits = 0xFFFFFFFFFFFFFFFFL;
		bb.putLong(clientIdBits);
		
		ClientId actual = BytesUtil.readClientId(bb.array(), 0);
		assertEquals(expected.getNamespaceId(), actual.getNamespaceId());
		assertEquals(expected.getClientIndex(), actual.getClientIndex());
	}
	
	@Test
	public void testReadClientId_NonZeroOffset() {
		ByteBuffer bb = ByteBuffer.allocate(16);
		
		ClientId expected = new ClientId(101, 888);
		long clientIdBits = 0x0065000000000378L;
		bb.putLong(1, clientIdBits);
		assertEquals(expected, BytesUtil.readClientId(bb.array(), 1));
	}
	
	@Test
	public void testWriteClientId() {
		byte[] array = new byte[8];
		ClientId expected = new ClientId(255, 100 * 1000 * 1000);
		
		BytesUtil.writeClientId(array, 0, expected);
		ByteBuffer bb = ByteBuffer.wrap(array);

		long expectedBits = 0x00FF000005F5E100L;
		assertEquals(expectedBits, bb.getLong());
	}
	
	@Test
	public void testWriteClientId_NonZeroOffset() {
		byte[] array = new byte[16];
		ClientId expected = new ClientId(255, 100 * 1000 * 1000);
		
		BytesUtil.writeClientId(array, 1, expected);
		ByteBuffer bb = ByteBuffer.wrap(array);
		
		long expectedBits = 0x00FF000005F5E100L;
		assertEquals(expectedBits, bb.getLong(1));
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_AllRaised() {
		byte[] array = new byte[1];
		array[0] = (byte) 0xff;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = BytesUtil.readFlagFromByte(array, 0, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}	
	
	@Test
	public void testReadFlagSingleByteFlagField_AllRaised_NonZeroOffset() {
		byte[] array = new byte[3];
		array[1] = (byte) 0xff;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = BytesUtil.readFlagFromByte(array, 1, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}
	
	@Test
	public void testReadFlagSingleByteFlagField_5thFlagRaised() {
		byte[] array = new byte[1];
		array[0] = (byte) 0x8;
		for (int i = 0; i < 8; i++) {
			boolean flagVal = BytesUtil.readFlagFromByte(array, 0, i);
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
			boolean flagVal = BytesUtil.readFlagFromByte(array, 1, i);
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
			boolean flagVal = BytesUtil.readFlagFromByte(array, 0, i);
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
			boolean flagVal = BytesUtil.readFlagFromByte(array, 1, i);
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
			boolean flagVal = BytesUtil.readFlagFromByte(array, 0, i);
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
			boolean flagVal = BytesUtil.readFlagFromByte(array, 1, i);
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
			BytesUtil.writeFlagToByte(array, 0, i, true);
		}
		assertEquals(0xFF, 0xFF & array[0]);
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_Raise3rd() {
		byte[] array = new byte[1];
		BytesUtil.writeFlagToByte(array, 0, 2, true);
		assertEquals(0x20, 0xFF & array[0]);
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_Raise1st() {
		byte[] array = new byte[1];
		BytesUtil.writeFlagToByte(array, 0, 0, true);
		assertEquals(0x80, 0xFF & array[0]);
	}
	
	@Test
	public void testWriteFlagSingleByteFlagField_RaiseLast() {
		byte[] array = new byte[1];
		BytesUtil.writeFlagToByte(array, 0, 7, true);
		assertEquals(0x1, 0xFF & array[0]);
	}
	
	@Test
	public void testReadFlag_AllRaised() {
		byte[] array = new byte[16];
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) 0xff;
		}
		for (int i = 0; i < 128; i++) {
			boolean flagVal = BytesUtil.readFlag(array, 0, 16, i);
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
			boolean flagVal = BytesUtil.readFlag(array, 6, 16, i);
			assertTrue(String.format("Flag index %d = %b", i, flagVal), flagVal);
		}
	}
	
	@Test
	public void testReadFlag_102ndFlagRaised() {
		byte[] array = new byte[16];
		array[12] = (byte) 0x4;
		for (int i = 0; i < 128; i++) {
			boolean flagVal = BytesUtil.readFlag(array, 0, 16, i);
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
			boolean flagVal = BytesUtil.readFlag(array, 6, 16, i);
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
			boolean flagVal = BytesUtil.readFlag(array, 0, 16, i);
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
			boolean flagVal = BytesUtil.readFlag(array, 6, 16, i);
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
			boolean flagVal = BytesUtil.readFlag(array, 0, 16, i);
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
			boolean flagVal = BytesUtil.readFlag(array, 6, 16, i);
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
			BytesUtil.writeFlag(array, 0, 16, i, true);
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
		BytesUtil.writeFlag(array, 0, 16, 73, true);
		assertLoneFlagIsRaised(73, array);
	}
	
	@Test
	public void testWriteFlag_Raise1st() {
		byte[] array = new byte[16];
		BytesUtil.writeFlag(array, 0, 16, 0, true);
		assertLoneFlagIsRaised(0, array);
	}
	
	@Test
	public void testWriteFlag_RaiseLast() {
		byte[] array = new byte[16];
		BytesUtil.writeFlag(array, 0, 16, 127, true);
		assertLoneFlagIsRaised(127, array);
	}
	
	
}
