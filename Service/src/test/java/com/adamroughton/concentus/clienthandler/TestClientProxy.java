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
package com.adamroughton.concentus.clienthandler;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestClientProxy {

	private ClientProxy _clientProxy;
	
	@Before
	public void setUp() {
		_clientProxy = new ClientProxy(0);
	}
	
	@Test
	public void lookupActionId_noActions() {
		assertEquals(-1, _clientProxy.lookupActionId(52));
	}
	
	@Test
	public void lookupActionId_oneMatchingAction() {
		_clientProxy.storeAssociation(0, 52);
		assertEquals(0, _clientProxy.lookupActionId(52));
	}
	
	@Test
	public void lookupActionId_oneNonMatchingAction() {
		_clientProxy.storeAssociation(0, 52);
		assertEquals(-1, _clientProxy.lookupActionId(50));
	}
	
	@Test
	public void lookupActionId_oneActionIndirectMatch() {
		_clientProxy.storeAssociation(0, 52);
		assertEquals(0, _clientProxy.lookupActionId(54));
	}
	
	@Test
	public void lookupActionId_manyActionsOneExactMatch() {
		long clientHandlerId = 5135;
		long[] ids = new long[100];
		for (int i = 0; i < 100; i++) {
			_clientProxy.storeAssociation(i, clientHandlerId);
			ids[i] = clientHandlerId;
			clientHandlerId += (i * 37 * 1000) % 10000;
		}
		assertEquals(97, _clientProxy.lookupActionId(ids[97]));
	}
	
	@Test
	public void lookupActionId_manyActionsOneIndirectMatch() {
		long clientHandlerId = 5135;
		long[] ids = new long[100];
		for (int i = 0; i < 100; i++) {
			_clientProxy.storeAssociation(i, clientHandlerId);
			ids[i] = clientHandlerId;
			clientHandlerId += (i * 37 * 1000) % 10000;
		}
		long indirectClientHandlerId = ids[97] + (ids[97] - ids[96]) / 2;
		assertEquals(97, _clientProxy.lookupActionId(indirectClientHandlerId));
	}
	
}
