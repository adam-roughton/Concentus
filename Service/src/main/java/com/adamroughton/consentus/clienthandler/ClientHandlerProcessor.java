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

import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.events.ClientConnectEvent;
import com.adamroughton.consentus.messaging.events.ClientInputEvent;
import com.adamroughton.consentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.consentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.consentus.messaging.events.StateInputEvent;
import com.adamroughton.consentus.messaging.events.StateUpdateEvent;
import com.adamroughton.consentus.messaging.patterns.EventReader;
import com.adamroughton.consentus.messaging.patterns.EventWriter;
import com.adamroughton.consentus.messaging.patterns.PubSendQueueWriter;
import com.adamroughton.consentus.messaging.patterns.RouterRecvQueueReader;
import com.adamroughton.consentus.messaging.patterns.RouterSendQueueWriter;
import com.adamroughton.consentus.messaging.patterns.SubRecvQueueReader;
import com.adamroughton.consentus.model.ClientId;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;

import static com.adamroughton.consentus.messaging.events.EventType.*;

public class ClientHandlerProcessor implements EventHandler<byte[]> {
	
	private final int _clientHandlerId;
	private final int _routerSocketId;
	private final int _subSocketId;
	
	private final PubSendQueueWriter _pubSendQueue;
	private final RouterSendQueueWriter _routerSendQueue;
	private final SubRecvQueueReader _subRecvQueueReader;
	private final RouterRecvQueueReader _routerRecvQueueReader;
	
	private final EventProcessingHeader _incomingQueueHeader;
	
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
			final RouterSendQueueWriter routerSendQueue,
			final PubSendQueueWriter pubSendQueue,
			final EventProcessingHeader incomingQueueHeader,
			final SubRecvQueueReader subRecvQueueReader,
			final RouterRecvQueueReader routerRecvQueueReader) {
		_clientHandlerId = clientHandlerId;
		_routerSocketId = routerSocketId;
		_subSocketId = subSocketId;
		
		_routerSendQueue = Objects.requireNonNull(routerSendQueue);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		_subRecvQueueReader = Objects.requireNonNull(subRecvQueueReader);
		_routerRecvQueueReader = Objects.requireNonNull(routerRecvQueueReader);
		
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
		int eventTypeId;
		
		if (recvSocketId == _routerSocketId) {
			eventTypeId = _routerRecvQueueReader.getEventType(eventBytes);
			final byte[] clientSocketId = _routerRecvQueueReader.getSocketId(eventBytes);
			if (eventTypeId == CLIENT_CONNECT.getId()) {
				_routerRecvQueueReader.read(eventBytes, _clientConnectEvent, new EventReader<ClientConnectEvent>() {

					@Override
					public void read(ClientConnectEvent event) {
						onClientConnected(clientSocketId, event);
					}
					
				});				
			} else if (eventTypeId == CLIENT_INPUT.getId()) {
				_routerRecvQueueReader.read(eventBytes, _clientInputEvent, new EventReader<ClientInputEvent>() {

					@Override
					public void read(ClientInputEvent event) {
						onClientInput(clientSocketId, event);
					}
				});
			} else {
				Log.warn(String.format("Unknown event type %d received on the router socket.", eventTypeId));
			}
		} else if (recvSocketId == _subSocketId) {
			eventTypeId = _subRecvQueueReader.getEventType(eventBytes);
			if (eventTypeId == STATE_UPDATE.getId()) {
				_subRecvQueueReader.read(eventBytes, _stateUpdateEvent, new EventReader<StateUpdateEvent>() {

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
		_routerSendQueue.send(newClient.getSocketId(), _connectResEvent, new EventWriter<ConnectResponseEvent>() {

			@Override
			public boolean write(ConnectResponseEvent event, long sequence) {
				event.setClientId(new ClientId(_clientHandlerId, newClientId));
				event.setResponseCode(ConnectResponseEvent.RES_OK);
				return true;
			}
			
		});
	}
	
	private void onClientInput(final byte[] clientSocketId, ClientInputEvent inputEvent) {
		//TODO 
		forwardStateInputEvent(clientSocketId, inputEvent);
	}
	
	private void onUpdateEvent(final StateUpdateEvent updateEvent) {
		//TODO need efficient data structure for this
		for (ClientProxy client : _clientLookup.values()) {
			final long nextUpdateId = client.getLastUpdateId() + 1;
			_routerSendQueue.send(client.getSocketId(), _clientUpdateEvent, new EventWriter<ClientUpdateEvent>() {

				@Override
				public boolean write(ClientUpdateEvent event, long sequence) {
					event.setUpdateId(nextUpdateId);
					event.setSimTime(updateEvent.getSimTime());
					// copy update data over
					updateEvent.copyUpdateBytes(event.getBackingArray(), 
							event.getUpdateOffset(), 
							event.getUpdateLength());
					return true;
				}
				
			});
			client.setLastUpdateId(nextUpdateId);
		}
	}
	

	private void forwardStateInputEvent(final byte[] clientSocketId, ClientInputEvent inputEvent) {
		// note that the event is currently ignored for testing
		_pubSendQueue.send(STATE_INPUT, _stateInputEvent, new EventWriter<StateInputEvent>() {

			@Override
			public boolean write(StateInputEvent event, long sequence) {
				event.setClientHandlerId(_clientHandlerId);
				event.setInputId(_inputId++);
				return true;
			}
			
		});
	}

}
