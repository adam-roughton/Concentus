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

import java.util.Objects;

import com.adamroughton.consentus.messaging.IncomingEventHeader;
import com.adamroughton.consentus.messaging.events.ClientConnectEvent;
import com.adamroughton.consentus.messaging.events.ClientInputEvent;
import com.adamroughton.consentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.consentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.consentus.messaging.events.StateInputEvent;
import com.adamroughton.consentus.messaging.events.StateUpdateEvent;
import com.adamroughton.consentus.messaging.patterns.EventPattern;
import com.adamroughton.consentus.messaging.patterns.EventReader;
import com.adamroughton.consentus.messaging.patterns.EventWriter;
import com.adamroughton.consentus.messaging.patterns.PubSubPattern;
import com.adamroughton.consentus.messaging.patterns.RouterPattern;
import com.adamroughton.consentus.messaging.patterns.SendQueue;
import com.adamroughton.consentus.model.ClientId;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;

import static com.adamroughton.consentus.messaging.events.EventType.*;

public class ClientHandlerProcessor implements EventHandler<byte[]> {
	
	private final int _clientHandlerId;
	private final int _routerSocketId;
	private final int _subSocketId;
	
	private final SendQueue _pubSendQueue;
	private final SendQueue _routerSendQueue;
	
	private final IncomingEventHeader _incomingQueueHeader;
	
	private long _inputId = 0;
	private long _nextClientId = 0;
	
	private final Long2ObjectMap<ClientProxy> _clientLookup = new Long2ObjectArrayMap<>(10000);
	
	private final StateInputEvent _stateInputEvent = new StateInputEvent();
	private final StateUpdateEvent _stateUpdateEvent = new StateUpdateEvent();
	private final ClientUpdateEvent _clientUpdateEvent = new ClientUpdateEvent();
	private final ConnectResponseEvent _connectResEvent = new ConnectResponseEvent();
	private final ClientConnectEvent _clientConnectEvent = new ClientConnectEvent();
	private final ClientInputEvent _clientInputEvent = new ClientInputEvent();
	
	public ClientHandlerProcessor(
			final int clientHandlerId,
			final int routerSocketId,
			final int subSocketId,
			final SendQueue routerSendQueue,
			final SendQueue pubSendQueue,
			final IncomingEventHeader incomingQueueHeader) {
		_clientHandlerId = clientHandlerId;
		_routerSocketId = routerSocketId;
		_subSocketId = subSocketId;
		
		_routerSendQueue = Objects.requireNonNull(routerSendQueue);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		
		_incomingQueueHeader = incomingQueueHeader;
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
	public void onEvent(byte[] eventBytes, long sequence, boolean endOfBatch)
			throws Exception {
		int recvSocketId = _incomingQueueHeader.getSocketId(eventBytes);
		int eventTypeId = EventPattern.getEventType(eventBytes, _incomingQueueHeader);
		
		if (recvSocketId == _routerSocketId) {
			final byte[] clientSocketId = RouterPattern.getSocketId(eventBytes, _incomingQueueHeader);
			if (eventTypeId == CLIENT_CONNECT.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _clientConnectEvent, new EventReader<ClientConnectEvent>() {

					@Override
					public void read(ClientConnectEvent event) {
						onClientConnected(clientSocketId, event);
					}
					
				});				
			} else if (eventTypeId == CLIENT_INPUT.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _clientInputEvent, new EventReader<ClientInputEvent>() {

					@Override
					public void read(ClientInputEvent event) {
						onClientInput(clientSocketId, event);
					}
				});
			} else {
				Log.warn(String.format("Unknown event type %d received on the router socket.", eventTypeId));
			}
		} else if (recvSocketId == _subSocketId) {
			if (eventTypeId == STATE_UPDATE.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _stateUpdateEvent, new EventReader<StateUpdateEvent>() {

					@Override
					public void read(StateUpdateEvent event) {
						onUpdateEvent(event);
					}
					
				});				
			} else {
				Log.warn(String.format("Unknown event type %d received on the sub socket.", eventTypeId));
			}
		} else {
			Log.warn(String.format("Unknown socket ID: %d", recvSocketId));
		}
	}
	
	private void onClientConnected(final byte[] clientSocketId, ClientConnectEvent connectEvent) {
		final long newClientId = _nextClientId++;
		ClientProxy newClient = new ClientProxy(newClientId);
		newClient.setLastMsgNanoTime(System.nanoTime());
		newClient.setSocketId(clientSocketId);
		
		_clientLookup.put(newClientId, newClient);
		
		// send a connect response
		_routerSendQueue.send(RouterPattern.asTask(newClient.getSocketId(), _connectResEvent, new EventWriter<ConnectResponseEvent>() {

			@Override
			public void write(ConnectResponseEvent event) {
				event.setClientId(new ClientId(_clientHandlerId, newClientId));
				event.setResponseCode(ConnectResponseEvent.RES_OK);
			}
			
		}));
	}
	
	private void onClientInput(final byte[] clientSocketId, ClientInputEvent inputEvent) {
		//TODO 
		forwardStateInputEvent(clientSocketId, inputEvent);
	}
	
	private void onUpdateEvent(final StateUpdateEvent updateEvent) {
		//TODO need efficient data structure for this
		for (ClientProxy client : _clientLookup.values()) {
			final long nextUpdateId = client.getLastUpdateId() + 1;
			_routerSendQueue.send(RouterPattern.asTask(client.getSocketId(), _clientUpdateEvent, new EventWriter<ClientUpdateEvent>() {

				@Override
				public void write(ClientUpdateEvent event) {
					event.setUpdateId(nextUpdateId);
					event.setSimTime(updateEvent.getSimTime());
					// copy update data over
					updateEvent.copyUpdateBytes(event.getBackingArray(), 
							event.getUpdateOffset(), 
							event.getUpdateLength());
				}
				
			}));
			client.setLastUpdateId(nextUpdateId);
		}
	}
	

	private void forwardStateInputEvent(final byte[] clientSocketId, ClientInputEvent inputEvent) {
		// note that the event is currently ignored for testing
		_pubSendQueue.send(PubSubPattern.asTask(_stateInputEvent, new EventWriter<StateInputEvent>() {

			@Override
			public void write(StateInputEvent event) {
				event.setClientHandlerId(_clientHandlerId);
				event.setInputId(_inputId++);
			}
			
		}));
	}

}
