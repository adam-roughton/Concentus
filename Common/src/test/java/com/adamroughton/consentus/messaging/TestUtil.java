package com.adamroughton.consentus.messaging;

import org.junit.*;

import com.adamroughton.consentus.Util;

import static org.junit.Assert.*;

public class TestUtil {

	@Test
	public void nextPowerOf2() {
		long expected = 1024;
		assertEquals(expected, Util.nextPowerOf2(567));
	}
	
	@Test
	public void nextPowerOf2_Boundary() {
		long expected = 512;
		assertEquals(expected, Util.nextPowerOf2(512));
	}
	
	@Test
	public void nextPowerOf2_Zero() {
		long expected = 1;
		assertEquals(expected, Util.nextPowerOf2(0));
	}
	
	@Test
	public void nextPowerOf2_JavaNegative() {
		int expected = 0;
		assertEquals(expected, Util.nextPowerOf2(0xA0000000));
	}
	
	@Test
	public void nextPowerOf2_PenultimateBitSet() {
		int expected = 0x80000000;
		assertEquals(expected, Util.nextPowerOf2(0x40000001));
	}
}
