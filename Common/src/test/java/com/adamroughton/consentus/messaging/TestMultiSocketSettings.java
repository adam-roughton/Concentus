package com.adamroughton.consentus.messaging;

import org.junit.Test;
import org.zeromq.ZMQ;

import static org.junit.Assert.*;

public class TestMultiSocketSettings {

	@Test
	public void singleSocketSetting() {
		SocketSettings socketSetting = SocketSettings.create(ZMQ.REQ);
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(socketSetting);
		
		assertEquals(1, multiSocketSettings.socketCount());
		assertFalse(multiSocketSettings.isSub(0));
		assertEquals(socketSetting, multiSocketSettings.getSocketSettings(0));
	}
	
	@Test
	public void singleSubSocketSetting() {
		SubSocketSettings subSocketSetting = SubSocketSettings.create(SocketSettings.create(ZMQ.SUB));
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(subSocketSetting);
		
		assertEquals(1, multiSocketSettings.socketCount());
		assertTrue(multiSocketSettings.isSub(0));
		assertEquals(subSocketSetting, multiSocketSettings.getSubSocketSettings(0));
	}
	
	@Test
	public void singleXSubSocketSetting() {
		SubSocketSettings xSubSocketSetting = SubSocketSettings.create(SocketSettings.create(ZMQ.XSUB));
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(xSubSocketSetting);
		
		assertEquals(1, multiSocketSettings.socketCount());
		assertTrue(multiSocketSettings.isSub(0));
		assertEquals(xSubSocketSetting, multiSocketSettings.getSubSocketSettings(0));
	}
	
	@Test(expected=RuntimeException.class)
	public void singleSocketSetting_trySub() {
		SocketSettings socketSetting = SocketSettings.create(ZMQ.REQ);
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(socketSetting);
		
		assertEquals(1, multiSocketSettings.socketCount());
		assertEquals(socketSetting, multiSocketSettings.getSubSocketSettings(0));
	}
	
	@Test
	public void singleSubSocketSetting_implicitWrap() {
		SocketSettings subSocketSetting = SocketSettings.create(ZMQ.SUB);
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(subSocketSetting);
		
		SubSocketSettings expected = SubSocketSettings.create(subSocketSetting)
				.subscribeToAll();
		
		assertEquals(1, multiSocketSettings.socketCount());
		assertTrue(multiSocketSettings.isSub(0));
		assertEquals(expected, multiSocketSettings.getSubSocketSettings(0));
	}
	
	@Test
	public void singleSubSocketSetting_asSocketSetting() {
		SocketSettings socketSetting = SocketSettings.create(ZMQ.SUB);
		SubSocketSettings subSocketSetting = SubSocketSettings.create(socketSetting);
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(subSocketSetting);
		
		assertEquals(1, multiSocketSettings.socketCount());
		assertTrue(multiSocketSettings.isSub(0));
		assertEquals(socketSetting, multiSocketSettings.getSocketSettings(0));
	}
	
	@Test
	public void multipleSocketSettingsSameType() {
		SocketSettings[] socketSettings = new SocketSettings[10];
		for (int i = 0; i < 10; i++) {
			socketSettings[i] = SocketSettings.create(ZMQ.REQ);
		}
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(socketSettings[0]);
		for (int i = 1; i < socketSettings.length; i++) {
			multiSocketSettings = multiSocketSettings.then(socketSettings[i]);
		}
		assertEquals(10, multiSocketSettings.socketCount());
		for (int i = 0; i < socketSettings.length; i++) {
			assertFalse(multiSocketSettings.isSub(i));
			assertEquals(socketSettings[i], multiSocketSettings.getSocketSettings(i));
		}
	}
	
	@Test
	public void multipleSocketSettingsDifferentTypes_alternating() {
		SocketSettings[] socketSettings = new SocketSettings[10];
		SubSocketSettings[] subSocketSettings = new SubSocketSettings[10];
		for (int i = 0; i < 10; i++) {
			socketSettings[i] = SocketSettings.create(ZMQ.REQ);
			subSocketSettings[i] = SubSocketSettings.create(SocketSettings.create(ZMQ.SUB));
		}
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(socketSettings[0]);
		int stdCount = 1;
		int subCount = 0;
		for (int i = 1; i < 20; i++) {
			if (i % 2 == 0) {
				multiSocketSettings = multiSocketSettings.then(socketSettings[stdCount++]);
			} else {
				multiSocketSettings = multiSocketSettings.then(subSocketSettings[subCount++]);
			}
		}
		assertEquals(20, multiSocketSettings.socketCount());
		for (int i = 0; i < socketSettings.length; i++) {
			if (i % 2 == 0) {
				assertFalse(multiSocketSettings.isSub(i));
				assertEquals(socketSettings[i], multiSocketSettings.getSocketSettings(i));
			} else {
				assertTrue(multiSocketSettings.isSub(i));
				assertEquals(subSocketSettings[i], multiSocketSettings.getSubSocketSettings(i));
			}
		}
	}
	
	@Test
	public void multipleSocketSettingsDifferentTypes_clustered() {
		SocketSettings[] socketSettings = new SocketSettings[10];
		SubSocketSettings[] subSocketSettings = new SubSocketSettings[10];
		for (int i = 0; i < 10; i++) {
			socketSettings[i] = SocketSettings.create(ZMQ.REQ);
			subSocketSettings[i] = SubSocketSettings.create(SocketSettings.create(ZMQ.SUB));
		}
		MultiSocketSettings multiSocketSettings = MultiSocketSettings.beginWith(socketSettings[0]);
		for (int i = 1; i < 5; i++) {
			multiSocketSettings = multiSocketSettings.then(socketSettings[i]);
		}
		for (int i = 0; i < subSocketSettings.length; i++) {
			multiSocketSettings = multiSocketSettings.then(subSocketSettings[i]);
		}
		for (int i = 5; i < socketSettings.length; i++) {
			multiSocketSettings = multiSocketSettings.then(socketSettings[i]);
		}
		assertEquals(20, multiSocketSettings.socketCount());
		for (int i = 0; i < socketSettings.length; i++) {
			if (i >= 0 && i < 5) {
				assertFalse(multiSocketSettings.isSub(i));
				assertEquals(socketSettings[i], multiSocketSettings.getSocketSettings(i));
			} else if (i >= 5 && i < 5 + subSocketSettings.length) {
				assertTrue(multiSocketSettings.isSub(i));
				assertEquals(subSocketSettings[i], multiSocketSettings.getSubSocketSettings(i));
			} else {
				assertFalse(multiSocketSettings.isSub(i));
				assertEquals(socketSettings[i], multiSocketSettings.getSocketSettings(i));
			}
		}
	}
	
}
