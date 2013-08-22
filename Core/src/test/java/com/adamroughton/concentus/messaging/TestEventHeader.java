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

import static org.junit.Assert.*;

import org.junit.Test;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.util.Util;

public class TestEventHeader {

	@Test
	public void testSegmentMetaData() {
		int expectedLength = 11;
		int expectedOffset = 56;
		int segmentMetaData = EventHeader.createSegmentMetaData(expectedOffset, expectedLength);
		assertEquals(expectedOffset, EventHeader.getSegmentOffset(segmentMetaData));
		assertEquals(expectedLength, EventHeader.getSegmentLength(segmentMetaData));
	}
	
	@Test
	public void testSegmentMetaDataMaxOffsetMaxLength() {
		int expectedLength = -1 & 0xFFFF;
		int expectedOffset = -1 & 0xFFFF;
		int segmentMetaData = EventHeader.createSegmentMetaData(expectedOffset, expectedLength);
		assertEquals(expectedOffset, EventHeader.getSegmentOffset(segmentMetaData));
		assertEquals(expectedLength, EventHeader.getSegmentLength(segmentMetaData));
	}
	
	@Test
	public void testSegmentMetaData0Offset0Length() {
		int expectedLength = 0;
		int expectedOffset = 0;
		int segmentMetaData = EventHeader.createSegmentMetaData(expectedOffset, expectedLength);
		assertEquals(expectedOffset, EventHeader.getSegmentOffset(segmentMetaData));
		assertEquals(expectedLength, EventHeader.getSegmentLength(segmentMetaData));
	}
	
	private void doTestResetHeader(int headerLength) {
		if (headerLength < 9) 
			throw new IllegalArgumentException("The header must be at least 9 bytes for flags and first section meta data.");
		
		int segmentCount = (headerLength - 5) / 4;
		int additionalLength = (headerLength - 5) % 4;
		
		EventHeader header = new EventHeader(0, segmentCount, additionalLength, 0) {
		};
		int arrayLen = Util.nextPowerOf2(headerLength);
		ArrayBackedResizingBuffer actualBuffer = new ArrayBackedResizingBuffer(arrayLen);
		byte[] actual = actualBuffer.getBuffer();
		ArrayBackedResizingBuffer expectedBuffer = new ArrayBackedResizingBuffer(arrayLen);
		byte[] expected = expectedBuffer.getBuffer();
		for (int i = 0; i < arrayLen; i++) {
			actual[i] = expected[i] = 9;
		}
		for (int i = 0; i < headerLength; i++) {
			expected[i] = 0;
		}
		header.reset(actualBuffer);
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testResetHeaderIntPlus1Bytes() {
		doTestResetHeader(9);
	}
	
	@Test
	public void testResetHeaderLongMinus1Bytes() {
		doTestResetHeader(11);
	}
	
	@Test
	public void testResetHeaderLongBytes() {
		doTestResetHeader(12);
	}

	@Test
	public void testResetHeaderLongPlus1Bytes() {
		doTestResetHeader(13);
	}
	
	@Test
	public void testResetHeaderIntPlusLongMinus1Bytes() {
		doTestResetHeader(15);
	}
	
	@Test
	public void testResetHeaderIntPlusLongBytes() {
		doTestResetHeader(16);
	}
	
	@Test
	public void testResetHeaderIntPlusLongPlus1Bytes() {
		doTestResetHeader(17);
	}
	
	@Test
	public void testResetHeader12LongBytes() {
		doTestResetHeader(100);
	}
	
	// negative segment length
	// negative offset
	
	
//	private class EventHeaderTestImpl extends EventHeader {
//
//		public EventHeaderTestImpl(int startOffset, int segmentCount,
//				int additionalLength, int additionalFlagCount) {
//			super(startOffset, segmentCount, additionalLength, additionalFlagCount);
//		}		
//		
//	}
	
}
