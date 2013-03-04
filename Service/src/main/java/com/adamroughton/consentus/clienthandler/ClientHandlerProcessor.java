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
import com.adamroughton.consentus.messaging.MessageBytesUtil;
import com.adamroughton.consentus.messaging.MessagePartBufferPolicy;
import com.adamroughton.consentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.consentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.consentus.messaging.events.StateInputEvent;
import com.adamroughton.consentus.messaging.events.StateUpdateEvent;
import com.adamroughton.consentus.model.ClientId;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import static com.adamroughton.consentus.messaging.events.EventType.*;

public class ClientHandlerProcessor implements EventHandler<byte[]> {

	/**
	 * The ID that enables this processor to recognise events received from
	 * the router socket.
	 */
	public final static int ROUTER_RECV_SOCKET_ID = 1;
	
	/**
	 * The ID that enables this processor to recognise events received from
	 * the subscription socket.
	 */
	public final static int SUB_RECV_SOCKET_ID = 2;
	
	public final static String SOCKET_ID_LABEL = "socketId";
	public final static String CONTENT_LABEL = "content";
	public final static String SUB_ID_LABEL = "subId";
	
	private final int _clientHandlerId;
	private final RingBuffer<byte[]> _pubSendQueue;
	private final RingBuffer<byte[]> _routerSendQueue;
	
	private final EventProcessingHeader _routerSocketHeader;
	private final EventProcessingHeader _subSocketHeader;
	private final EventProcessingHeader _pubSocketHeader;
	
	private final MessagePartBufferPolicy _routerPolicy;
	private final MessagePartBufferPolicy _subPolicy;
	private final MessagePartBufferPolicy _pubPolicy;
	
	private final int _routerSocketIdOffset;
	private final int _routerSocketIdLength;
	private final int _routerContentOffset;
	private final int _routerContentLength;
	
	private final int _subIdOffset;
	private final int _subContentOffset;
	
	private long _inputId = 0;
	private long _nextClientId = 0;
	
	private final Long2ObjectMap<ClientProxy> _clientLookup = new Long2ObjectArrayMap<>(10000);
	
	private final StateInputEvent _stateInputEvent = new StateInputEvent();
	private final StateUpdateEvent _stateUpdateEvent = new StateUpdateEvent();
	private final ClientUpdateEvent _clientUpdateEvent = new ClientUpdateEvent();
	private final ConnectResponseEvent _connectResEvent = new ConnectResponseEvent();
	
	public ClientHandlerProcessor(
			final int clientHandlerId,
			final RingBuffer<byte[]> routerSendQueue,
			final RingBuffer<byte[]> pubSendQueue, 
			final EventProcessingHeader routerSocketHeader,
			final EventProcessingHeader subSocketHeader,
			final EventProcessingHeader pubSocketHeader,
			final MessagePartBufferPolicy routerPolicy,
			final MessagePartBufferPolicy subPolicy,
			final MessagePartBufferPolicy pubPolicy) {
		_clientHandlerId = clientHandlerId;
		
		_routerSendQueue = Objects.requireNonNull(routerSendQueue);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		
		_routerSocketHeader = Objects.requireNonNull(routerSocketHeader);
		_subSocketHeader = Objects.requireNonNull(subSocketHeader);
		_pubSocketHeader = Objects.requireNonNull(pubSocketHeader);
		
		_routerPolicy = Objects.requireNonNull(routerPolicy);
		_subPolicy = Objects.requireNonNull(subPolicy);
		_pubPolicy = Objects.requireNonNull(pubPolicy);
		
		int availableRouterBufferLength = routerSendQueue.get(0).length - routerSocketHeader.getLength();
		
		int routerSocketIdPartIndex = routerPolicy.getPartIndex(SOCKET_ID_LABEL);
		_routerSocketIdOffset = routerSocketHeader.getEventOffset() + routerPolicy.getOffset(routerSocketIdPartIndex);
		_routerSocketIdLength = routerPolicy.getPartLength(routerSocketIdPartIndex, availableRouterBufferLength);
		
		int routerContentPartIndex = routerPolicy.getPartIndex(CONTENT_LABEL);
		_routerContentOffset = routerSocketHeader.getEventOffset() + routerPolicy.getOffset(routerContentPartIndex);
		_routerContentLength = routerPolicy.getPartLength(routerContentPartIndex, availableRouterBufferLength);
		
		int subIdPartIndex = subPolicy.getPartIndex(SUB_ID_LABEL);
		_subIdOffset = subPolicy.getOffset(subIdPartIndex);
		
		int subContentIndex = subPolicy.getPartIndex(CONTENT_LABEL);
		_subContentOffset = subPolicy.getOffset(subContentIndex);
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
		int eventTypeId;
		
		if (socketId == ROUTER_RECV_SOCKET_ID) {
			eventTypeId = MessageBytesUtil.readInt(event, _routerSocketHeader.getEventOffset());
			if (eventTypeId == CLIENT_CONNECT.getId()) {
				onClientConnected(event);
			} else if (eventTypeId == CLIENT_INPUT.getId()) {
				onClientInput(event);
			}
		} else if (socketId == SUB_RECV_SOCKET_ID) {
			eventTypeId = MessageBytesUtil.readInt(event, _subSocketHeader.getEventOffset());
			if (eventTypeId == STATE_UPDATE.getId()) {
				onUpdateEvent(event);
			}
		}
		//TODO: handle unrecognised events
	}
	
	private void onClientConnected(byte[] event) {
		long newClientId = _nextClientId++;
		ClientProxy newClient = new ClientProxy(newClientId);
		newClient.setLastMsgNanoTime(System.nanoTime());
		
		newClient.setSocketId(event, _routerSocketIdOffset, _routerSocketIdLength);
		
		_clientLookup.put(newClientId, newClient);
		
		long sendSeq = _routerSendQueue.next();
		try {
			byte[] sendBytes = _routerSendQueue.get(sendSeq);
			_connectResEvent.setBackingArray(sendBytes, _routerSocketHeader.getEventOffset());
			_connectResEvent.setClientId(new ClientId(_clientHandlerId, newClientId));
			_connectResEvent.setResponseCode(ConnectResponseEvent.RES_OK);
			_routerSocketHeader.setIsValid(true, sendBytes);
		} finally {
			_routerSendQueue.publish(sendSeq);
			_connectResEvent.releaseBackingArray();
		}
	}

	private void onClientInput(byte[] event) {
		forwardStateInputEvent(event);
	}
	
	private void onUpdateEvent(byte[] event) {
		try {
			_stateUpdateEvent.setBackingArray(event, _subSocketHeader.getEventOffset());
			//TODO need efficient data structure for this
			for (ClientProxy client : _clientLookup.values()) {
				long routerSeq = _routerSendQueue.next();
				try { 
					byte[] sendBytes = _routerSendQueue.get(routerSeq);
					client.writeSocketId(sendBytes, _routerSocketIdOffset);
					_clientUpdateEvent.setBackingArray(sendBytes, _routerContentOffset);
					
					long nextUpdateId = client.getLastUpdateId() + 1;
					_clientUpdateEvent.setUpdateId(nextUpdateId);
					
					_clientUpdateEvent.setSimTime(_stateUpdateEvent.getSimTime());
					_stateUpdateEvent.copyUpdateBytes(sendBytes, 
							_clientUpdateEvent.getUpdateOffset(), 
							_clientUpdateEvent.getUpdateLength());
					
					_routerSocketHeader.setIsValid(true, sendBytes);
					client.setLastUpdateId(nextUpdateId);
				} finally {
					_routerSendQueue.publish(routerSeq);
					_clientUpdateEvent.releaseBackingArray();
				}
			}
		} finally {
			_stateUpdateEvent.releaseBackingArray();
		}
	}
	

	private void forwardStateInputEvent(byte[] event) {
		long pubSeq = _pubSendQueue.next();
		try {
			byte[] pubBytes = _pubSendQueue.get(pubSeq);
			_stateInputEvent.setBackingArray(pubBytes, _pubSocketHeader.getEventOffset());
			_stateInputEvent.setClientHandlerId(_clientHandlerId);
			_stateInputEvent.setInputId(_inputId++);
			_pubSocketHeader.setIsValid(true, pubBytes);
		} finally {
			_pubSendQueue.publish(pubSeq);
			_stateInputEvent.releaseBackingArray();
		}
	}

}
