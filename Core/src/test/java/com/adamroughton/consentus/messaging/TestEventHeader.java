package com.adamroughton.consentus.messaging;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

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
