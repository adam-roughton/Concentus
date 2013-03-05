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
package com.adamroughton.consentus.crowdhammer.worker;

import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.zeromq.ZMQ;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TestClient {
	
	private Client _client;
	@Mock private ZMQ.Context _zmqContext;
	
	@Before
	public void setUp() {
		_client = new Client(_zmqContext);
	}
	
	@Test
	public void sendActionQueue() {
		for (int i = 0; i < Client.SEND_BUFFER_SIZE; i++) {
			assertEquals(i, _client.addSentAction(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _client.getSentTime(i));
		}
	}
	
	@Test
	public void sendActionQueue_WrapAround() {
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			assertEquals(i, _client.addSentAction(i * 1000 * 1000));
			assertEquals(i * 1000 * 1000, _client.getSentTime(i));
		}
	}
	
	@Test
	public void sendActionQueue_FetchLastEntry() {
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			assertEquals(i, _client.addSentAction(i * 1000 * 1000));
			if (i - Client.SEND_BUFFER_SIZE >= 0) {
				int lastAvailableEntry = i - Client.SEND_BUFFER_SIZE + 1;
				assertEquals(lastAvailableEntry * 1000 * 1000, _client.getSentTime(lastAvailableEntry));
			}
		}
	}
	
	@Test
	public void sendActionQueue_FetchSecondEntry() {
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			assertEquals(i, _client.addSentAction(i * 1000 * 1000));
			if (i - 1 >= 0) {
				int secondEntry = i - 1;
				assertEquals(secondEntry * 1000 * 1000, _client.getSentTime(secondEntry));
			}
		}
	}
	
	@Test
	public void sendActionQueue_MidEntry() {
		int midPoint = Client.SEND_BUFFER_SIZE / 2 + 1;
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			assertEquals(i, _client.addSentAction(i * 1000 * 1000));
			if (i - midPoint >= 0) {
				int midEntry = i - midPoint;
				assertEquals(midEntry * 1000 * 1000, _client.getSentTime(midEntry));
			}
		}
	}
	
	@Test
	public void getSentTime_NotInitialised() {		
		assertEquals(-1, _client.getSentTime(325));
	}
	
	@Test
	public void getSentTime_InFuture() {
		long lastClientEntry = -1;
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			lastClientEntry = _client.addSentAction(i * 1000 * 1000);
		}
		assertEquals(-1, _client.getSentTime(lastClientEntry + 324));
	}
	
	@Test
	public void getSentTime_TooFarInPast() {
		long lastClientEntry = -1;
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			lastClientEntry = _client.addSentAction(i * 1000 * 1000);
		}
		assertEquals(-1, _client.getSentTime(lastClientEntry - ((3 * Client.SEND_BUFFER_SIZE) / 2)));
	}
	
	@Test
	public void getSentTime_Negative() {
		for (int i = 0; i < 2 * Client.SEND_BUFFER_SIZE; i++) {
			_client.addSentAction(i * 1000 * 1000);
		}
		assertEquals(-1, _client.getSentTime(-300));
	}
	
	
}
