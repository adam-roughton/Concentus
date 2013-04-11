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

import it.unimi.dsi.fastutil.longs.Long2LongArrayMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;

import java.util.Objects;

import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.consentus.messaging.IncomingEventHeader;
import com.adamroughton.consentus.messaging.MultiSocketOutgoingEventHeader;
import com.adamroughton.consentus.messaging.OutgoingEventHeader;
import com.adamroughton.consentus.messaging.events.ClientConnectEvent;
import com.adamroughton.consentus.messaging.events.ClientInputEvent;
import com.adamroughton.consentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.consentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.consentus.messaging.events.EventType;
import com.adamroughton.consentus.messaging.patterns.EventPattern;
import com.adamroughton.consentus.messaging.patterns.EventReader;
import com.adamroughton.consentus.messaging.patterns.EventWriter;
import com.adamroughton.consentus.messaging.patterns.SendQueue;
import com.lmax.disruptor.LifecycleAware;

import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.consentus.Constants.*;

public class SimulatedClientProcessor implements DeadlineBasedEventHandler<byte[]>, LifecycleAware {

	private final Long2LongMap _clientsIndex;
	private final StructuredArray<Client> _clients;
	private final long _clientLengthMask;
	
	private volatile boolean _isSendingInput = false;
	
	private final SendQueue<MultiSocketOutgoingEventHeader> _clientSendQueue;
	private final SendQueue<OutgoingEventHeader> _metricSendQueue;	
	private final IncomingEventHeader _recvHeader;
	
	private final ClientConnectEvent _connectEvent = new ClientConnectEvent();
	private final ConnectResponseEvent _connectRes = new ConnectResponseEvent();
	private final ClientInputEvent _inputEvent = new ClientInputEvent();
	private final ClientUpdateEvent _updateEvent = new ClientUpdateEvent();
	
	private long _nextClientIndex = -1;
	private long _nextMetricBucketId;
	private boolean _sendMetric = false;
	
	// metrics
	private int _connectedClientCount = 0;
	
	public SimulatedClientProcessor(final StructuredArray<Client> clients, 
			final SendQueue<MultiSocketOutgoingEventHeader> clientSendQueue,
			final SendQueue<OutgoingEventHeader> metricSendQueue,
			final IncomingEventHeader recvHeader) {
		_clients = Objects.requireNonNull(clients);
		long clientsLength = _clients.getLength();
		if (Long.bitCount(clientsLength) != 1) 
			throw new IllegalArgumentException("The allocated client array must be a power of 2.");
		_clientLengthMask = clientsLength - 1;
		
		// create an index for quickly looking up clients
		_clientsIndex = new Long2LongArrayMap((int)_clients.getLength());
		for (long clientIndex = 0; clientIndex < _clients.getLength(); clientIndex++) {
			_clientsIndex.put(_clients.get(clientIndex).getClientId(), clientIndex);
		}
		
		_clientSendQueue = Objects.requireNonNull(clientSendQueue);
		_metricSendQueue = Objects.requireNonNull(metricSendQueue);
		_recvHeader = Objects.requireNonNull(recvHeader);
	}
	
	@Override
	public void onStart() {
		_isSendingInput = true;
		_nextMetricBucketId = Util.getCurrentMetricBucketId();
	}

	@Override
	public void onShutdown() {
	}
	
	@Override
	public void onEvent(byte[] event, long sequence, long nextDeadline)
			throws Exception {
		if (EventPattern.getEventType(event, _recvHeader) == EventType.CLIENT_UPDATE.getId()) {
			EventPattern.readContent(event, _recvHeader, _updateEvent, new EventReader<IncomingEventHeader, ClientUpdateEvent>() {

				@Override
				public void read(IncomingEventHeader header, ClientUpdateEvent event) {
					long clientId = event.getClientId();
					long clientIndex = _clientsIndex.get(clientId);
					Client updatedClient = _clients.get(clientIndex);
					
					processUpdateEvent(updatedClient, event);
				}
			});
		} else if (EventPattern.getEventType(event, _recvHeader) == EventType.CONNECT_RES.getId()) {
			EventPattern.readContent(event, _recvHeader, _connectRes, new EventReader<IncomingEventHeader, ConnectResponseEvent>() {

				@Override
				public void read(IncomingEventHeader header, ConnectResponseEvent event) {
					// we use the index of the connecting client as the request ID
					long clientIndex = event.getCallbackBits();
					Client connectedClient = _clients.get(clientIndex);
					processConnectRes(connectedClient, event);
				}
			});
		}
	}

	@Override
	public void onDeadline() {
		if (_sendMetric) {
			sendMetricEvent();
		} else if (_isSendingInput) {
			Client client = _clients.get(_nextClientIndex);	
			if (client.hasConnected()) {		
				// send outgoing event
				sendInputEvent(client);
				client.advanceSendTime();
			} else if (!client.isConnecting()) {
				// send connect request
				sendConnectRequest(_nextClientIndex, client);
				client.setIsConnecting(true);
			}
			// if we are waiting to connect, do nothing with this client
		}
	}

	@Override
	public long moveToNextDeadline(long forcedEventCount) {
		if (!_sendMetric) {
			// if we didn't send a metric on the last deadline, advance for the next client
			do {
				// find the next active client
				_nextClientIndex = (_nextClientIndex + 1) & _clientLengthMask;
			} while(!_clients.get(_nextClientIndex).isActive());
		}
		
		long nextClientDeadline = _clients.get(_nextClientIndex).getNextSendTimeInMillis();
		long nextMetricDeadline = Util.getMetricBucketEndTime(_nextMetricBucketId);
		if (nextMetricDeadline < nextClientDeadline) {
			_sendMetric = true;
			return nextMetricDeadline;
		} else {
			_sendMetric = false;
			return nextClientDeadline;
		}		
	}

	@Override
	public long getDeadline() {
		if (_sendMetric) {
			return Util.getMetricBucketEndTime(_nextMetricBucketId);
		} else {
			return _clients.get(_nextClientIndex).getNextSendTimeInMillis();
		}
	}
	
	public void stopSendingInput() {
		_isSendingInput = false;
	}
	
	private void processUpdateEvent(final Client client, final ClientUpdateEvent updateEvent) {
		client.getUpdateIdToRecvTimeMap().put(updateEvent.getUpdateId(), System.currentTimeMillis());
	}
	
	private void sendConnectRequest(final long clientIndex, final Client client) {
		_clientSendQueue.send(EventPattern.asTask(_connectEvent, new EventWriter<MultiSocketOutgoingEventHeader, ClientConnectEvent>() {

			@Override
			public void write(MultiSocketOutgoingEventHeader header, ClientConnectEvent event) throws Exception {
				header.setTargetSocketId(event.getBackingArray(), client.getHandlerId());
				event.setCallbackBits(clientIndex);
			}
			
		}));
	}
	
	private void sendInputEvent(final Client client) {
		_clientSendQueue.send(EventPattern.asTask(_inputEvent, new EventWriter<MultiSocketOutgoingEventHeader, ClientInputEvent>() {

			@Override
			public void write(MultiSocketOutgoingEventHeader header, ClientInputEvent event) throws Exception {
				header.setTargetSocketId(event.getBackingArray(), client.getHandlerId());
				long sendTime = System.nanoTime();
				long actionId = client.getSentIdToSentTimeMap().add(sendTime);
				event.setClientId(client.getClientId());
				event.setClientActionId(actionId);	
			}
			
		}));
	}
	
	private void sendMetricEvent() {
		
	}
	
	private void processConnectRes(final Client client, final ConnectResponseEvent resEvent) {
		if (!client.isConnecting()) 
			throw new RuntimeException("Expected the client to be connecting on reception of a connect response event.");
		if (resEvent.getResponseCode() != ConnectResponseEvent.RES_OK) {
			throw new RuntimeException(String.format("The response code for a client connection was %d, expected %d (OK). Aborting test", 
					resEvent.getResponseCode(), ConnectResponseEvent.RES_OK));
		}
		client.setClientId(resEvent.getClientIdBits());
		client.setIsConnecting(false);
		_connectedClientCount++;
	}
	
}
