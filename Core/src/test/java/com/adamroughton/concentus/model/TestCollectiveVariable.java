package com.adamroughton.concentus.model;

import com.adamroughton.concentus.messaging.MessageBytesUtil;

import org.junit.Test;
import static junit.framework.Assert.*;

public class TestCollectiveVariable {

	@Test
	public void sameLengthOtherAllHigherMerge() {
		CollectiveVariable var1 = new CollectiveVariable(5, 0);
		CollectiveVariable var2 = new CollectiveVariable(5, 0);
		
		for (int i = 0; i < 5; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var1.push(new CandidateValue(0, i * 10, data));
		}
		for (int i = 5; i < 10; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var2.push(new CandidateValue(0, i * 10, data));
		}
		
		CollectiveVariable res = var1.union(var2);
		
		assertEquals(5, res.getValueCount());
		for (int i = 0; i < 5; i++) {
			byte[] expData = new byte[4];
			MessageBytesUtil.writeInt(expData, 0, (9 - i));
			assertEquals(new CandidateValue(0, (9 - i) * 10, expData), res.getValue(i));
		}
	}
	
	@Test
	public void sameLengthMixedScoresMerge() {
		CollectiveVariable var1 = new CollectiveVariable(5, 0);
		CollectiveVariable var2 = new CollectiveVariable(5, 0);
		
		for (int i = 0; i < 10; i++) {
			byte[] data = new byte[4];
			if (i % 2 == 0) {
				MessageBytesUtil.writeInt(data, 0, i);
				var1.push(new CandidateValue(0, i * 10, data));
			} else {
				MessageBytesUtil.writeInt(data, 0, i);
				var2.push(new CandidateValue(0, i * 10, data));
			}
		}
		
		CollectiveVariable res = var1.union(var2);
		
		assertEquals(5, res.getValueCount());
		for (int i = 0; i < 5; i++) {
			byte[] expData = new byte[4];
			MessageBytesUtil.writeInt(expData, 0, (9 - i));
			assertEquals(new CandidateValue(0, (9 - i) * 10, expData), res.getValue(i));
		}
	}
	
	@Test
	public void differentLengthMergeOnLarge() {
		CollectiveVariable var1 = new CollectiveVariable(7, 0);
		CollectiveVariable var2 = new CollectiveVariable(5, 0);
		
		for (int i = 0; i < 7; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var1.push(new CandidateValue(0, i * 10, data));
		}
		for (int i = 7; i < 12; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var2.push(new CandidateValue(0, i * 10, data));
		}
		
		CollectiveVariable res = var1.union(var2);
		
		assertEquals(7, res.getValueCount());
		for (int i = 0; i < 7; i++) {
			byte[] expData = new byte[4];
			MessageBytesUtil.writeInt(expData, 0, (11 - i));
			assertEquals(new CandidateValue(0, (11 - i) * 10, expData), res.getValue(i));
		}
	}
	
	@Test
	public void differentLengthMergeOnSmall() {
		CollectiveVariable var1 = new CollectiveVariable(7, 0);
		CollectiveVariable var2 = new CollectiveVariable(5, 0);
		
		for (int i = 0; i < 7; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var1.push(new CandidateValue(0, i * 10, data));
		}
		for (int i = 7; i < 12; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var2.push(new CandidateValue(0, i * 10, data));
		}
		
		CollectiveVariable res = var2.union(var1);
		
		assertEquals(5, res.getValueCount());
		for (int i = 0; i < 5; i++) {
			byte[] expData = new byte[4];
			MessageBytesUtil.writeInt(expData, 0, (11 - i));
			assertEquals(new CandidateValue(0, (11 - i) * 10, expData), res.getValue(i));
		}
	}
	
	@Test
	public void mergeOnZeroLengthWithNonZeroLength() {
		CollectiveVariable var1 = new CollectiveVariable(0, 0);
		CollectiveVariable var2 = new CollectiveVariable(5, 0);
		
		for (int i = 7; i < 12; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var2.push(new CandidateValue(0, i * 10, data));
		}
		
		CollectiveVariable res = var1.union(var2);
		
		assertEquals(0, res.getValueCount());
	}
	
	@Test
	public void mergeOnZeroLengthBothZeroLength() {
		CollectiveVariable var1 = new CollectiveVariable(0, 0);
		CollectiveVariable var2 = new CollectiveVariable(0, 0);
		
		CollectiveVariable res = var1.union(var2);
		
		assertEquals(0, res.getValueCount());
	}
	
	@Test
	public void mergeOnNonZeroLengthWithZeroLength() {
		CollectiveVariable var1 = new CollectiveVariable(5, 0);
		CollectiveVariable var2 = new CollectiveVariable(0, 0);
		
		for (int i = 0; i < 5; i++) {
			byte[] data = new byte[4];
			MessageBytesUtil.writeInt(data, 0, i);
			var1.push(new CandidateValue(0, i * 10, data));
		}
		
		CollectiveVariable res = var1.union(var2);
		
		assertEquals(5, res.getValueCount());
		for (int i = 0; i < 5; i++) {
			byte[] expData = new byte[4];
			MessageBytesUtil.writeInt(expData, 0, (4 - i));
			assertEquals(new CandidateValue(0, (4 - i) * 10, expData), res.getValue(i));
		}
	}
	
}
