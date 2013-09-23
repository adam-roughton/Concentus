package com.adamroughton.concentus.data.model;

import java.util.Arrays;

import org.junit.Test;

import com.adamroughton.concentus.data.model.kryo.CandidateValue;

import static org.junit.Assert.*;

public class TestCandidateValueCompareTo {

	@Test
	public void differentVarIds() {
		CandidateValue val1 = newValue(0, 100, 0, 0, 0, 1);
		CandidateValue val2 = newValue(1, 100, 0, 0, 0, 1);
		CandidateValue val3 = newValue(2, 100, 0, 0, 0, 1);
		assertComparableCorrect(val1, val2, val3);
	}
	
	@Test
	public void differentScores() {
		CandidateValue val1 = newValue(0, 100, 0, 0, 0, 1);
		CandidateValue val2 = newValue(0, 105, 0, 0, 0, 1);
		CandidateValue val3 = newValue(0, 110, 0, 0, 0, 1);
		assertComparableCorrect(val1, val2, val3);
	}
	
	@Test
	public void differentData() {
		CandidateValue val1 = newValue(0, 100, 255, 0, 0, 0);
		CandidateValue val2 = newValue(0, 100, 0, 255, 0, 0);
		CandidateValue val3 = newValue(0, 100, 0, 0, 255, 0);
		assertComparableCorrect(val1, val2, val3);
	}
	
	@Test
	public void differentDataSameHash() {
		CandidateValue val1 = newValue(0, 100, 0, 0, 30, 32);
		CandidateValue val2 = newValue(0, 100, 0, 0, 31, 1);
		CandidateValue val3 = newValue(0, 100, 0, 1, 0, 1);
		
		assertEquals(Arrays.hashCode(new byte[] {0, 1, 0, 1}), 
				Arrays.hashCode(new byte[] {0, 0, 31, 1}));
		assertEquals(Arrays.hashCode(new byte[] {0, 0, 31, 1}), 
				Arrays.hashCode(new byte[] {0, 0, 30, 32}));
		
		assertComparableCorrect(val1, val2, val3);
	}
	
	@Test
	public void equal() {
		CandidateValue val1 = newValue(0, 100, 0, 0, 0, 1);
		CandidateValue val2 = newValue(0, 100, 0, 0, 0, 1);
		assertEquals(0, val1.compareTo(val2));
		assertEquals(val1.compareTo(val2), -val2.compareTo(val1));
	}
	
	private void assertComparableCorrect(CandidateValue first, CandidateValue second, CandidateValue third) {
		assertTrue(first.compareTo(second) > 0);
		assertTrue(second.compareTo(third) > 0);
		assertTrue(first.compareTo(third) > 0);
		assertEquals(first.compareTo(second), -second.compareTo(first));
		assertEquals(second.compareTo(third), -third.compareTo(second));
		assertEquals(first.compareTo(third), -third.compareTo(first));
	}
	
	private CandidateValue newValue(int varId, int score, long...bytes) {
		byte[] data = new byte[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			data[i] = (byte) bytes[i];
		}
		return new CandidateValue(varId, score, data);
	}
	
}
