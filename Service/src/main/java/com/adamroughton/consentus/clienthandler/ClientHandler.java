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
package com.adamroughton.consentus.clienthandler;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.lmax.disruptor.EventHandler;

public class ClientHandler implements EventHandler<byte[]> {

	// might be better to preallocate 20,000 slots and use this
	private final Long2ObjectMap<ClientProxy> _clientProxyLookup;
	private long _nextClientId = 0;
		
	public ClientHandler() {
		_clientProxyLookup = new Long2ObjectArrayMap<>();
	}
	
	/**
	 * We want a free list containing clients who have free connections available
	 * this list should be sorted by the number of connections
	 * 
	 * when a new client is added:
	 * 1. search on this handler for the free slots first (i.e. prefer to cluster neighbours on the
	 * same node)
	 * 2. if slots are not available, round-robin on the other client handlers for free connections until
	 * either the quota is filled or none are found
	 * 3. if the quote is not filled, add this client to the free slots list
	 */
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean endOfBatch)
			throws Exception {
		int socketId = MessageBytesUtil.read4BitUInt(event, 0, 4);
		switch (socketId) {
		
		}
		
		
		
		// need to identify message type
		// need address (lookup client proxy with this) - add security later (e.g. token)
		
		// add client
		// remove client
		// new joint action
		//	: inform neighbours, send event to canonical service
		
		// select joint action
		// unselect joint action
		// action
		
		
		// clientId : {clientHandlerID:internalId}
		
		// assume authentication system separate to the client handlers
		// therefore the client handler gets given 
		/*
		 * connection:
		 *  1. get free client slot count (count)
		 * 	2. request client proxy slot (slotID, failure)
		 * 	3. client connects with ID = {clientHandlerId:slotID}
		 * 
		 * for testing:
		 *  1. client connects -> internal ID
		 *  2. proxy stores mapping from internal ID -> proxy ID
		 *  3. 
		 * 
		 * event flow:
		 *  
		 * 
		 */
		
		
	}

	private void onDirectEvent(byte[] event, long sequence) {
		
	}
	
	private void onUpdateEvent(byte[] event, long sequence) {
		
	}
}
