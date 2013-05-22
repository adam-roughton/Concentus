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

import java.util.Objects;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.MetricContainer;
import com.adamroughton.concentus.MetricContainer.MetricLamda;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientConnectEvent;
import com.adamroughton.concentus.messaging.events.ClientHandlerMetricEvent;
import com.adamroughton.concentus.messaging.events.ClientInputEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.events.StateInputEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateInfoEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.RouterPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.model.ClientId;
import com.esotericsoftware.minlog.Log;

import static com.adamroughton.concentus.messaging.events.EventType.*;

public class ClientHandlerProcessor implements DeadlineBasedEventHandler<byte[]> {
	
	private final Clock _clock;
	
	private final int _clientHandlerId;
	private final int _routerSocketId;
	private final int _subSocketId;
	
	private final SendQueue<OutgoingEventHeader> _pubSendQueue;
	private final SendQueue<OutgoingEventHeader> _routerSendQueue;
	private final SendQueue<OutgoingEventHeader> _metricSendQueue;
	
	private final UpdateHandler _updateHandler = new UpdateHandler(32);
	private final IncomingEventHeader _incomingQueueHeader;
	
	private long _inputId = 0;
	private long _nextClientId = 0;
	private long _connectedClientCount = 0;
	
	private final StructuredArray<ClientProxy> _clientLookup = StructuredArray.newInstance(128 * 1024, ClientProxy.class, new ComponentFactory<ClientProxy>() {

		private long nextClientId = 0;
		
		@Override
		public ClientProxy newInstance(Object[] initArgs) {
			return new ClientProxy(nextClientId++);
		}
	});
	//private final Long2ObjectMap<ClientProxy> _clientLookup = new Long2ObjectRBTreeMap<>();// new Long2ObjectArrayMap<>(10000);
	
	private final StateInputEvent _stateInputEvent = new StateInputEvent();
	private final StateUpdateEvent _stateUpdateEvent = new StateUpdateEvent();
	private final StateUpdateInfoEvent _updateInfoEvent = new StateUpdateInfoEvent();
	private final ConnectResponseEvent _connectResEvent = new ConnectResponseEvent();
	private final ClientConnectEvent _clientConnectEvent = new ClientConnectEvent();
	private final ClientInputEvent _clientInputEvent = new ClientInputEvent();
	private final ClientHandlerMetricEvent _metricEvent = new ClientHandlerMetricEvent();
	
	private final MetricContainer<MetricEntry> _metricContainer;
	private long _lastSentMetricBucketId = -1;
	private long _nextMetricTime = -1;
	
	private static class MetricEntry {
		public int eventsRecvd = 0;
		public int inputActionProcessed = 0;
		public int connectionRequestsProcessed = 0;
		public int updatesProcessed = 0;
		public int updateInfoEventsProcessed = 0;
		public int sentUpdateCount = 0;
		public long pendingEventCount = 0;
	}
	
	public ClientHandlerProcessor(
			final Clock clock,
			final int clientHandlerId,
			final int routerSocketId,
			final int subSocketId,
			final SendQueue<OutgoingEventHeader> routerSendQueue,
			final SendQueue<OutgoingEventHeader> pubSendQueue,
			final SendQueue<OutgoingEventHeader> metricSendQueue,
			final IncomingEventHeader incomingQueueHeader) {
		_clock = Objects.requireNonNull(clock);
		_clientHandlerId = clientHandlerId;
		_routerSocketId = routerSocketId;
		_subSocketId = subSocketId;
		
		_routerSendQueue = Objects.requireNonNull(routerSendQueue);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		_metricSendQueue = Objects.requireNonNull(metricSendQueue);
		
		_incomingQueueHeader = incomingQueueHeader;
		
		_metricContainer = new MetricContainer<>(clock, 8,
				MetricEntry.class,
				new ComponentFactory<MetricEntry>() {
		
					@Override
					public MetricEntry newInstance(Object[] initArgs) {
						return new MetricEntry();
					}
					
				}, new InitialiseDelegate<MetricEntry>() {
		
					@Override
					public void initialise(MetricEntry content) {
						content.eventsRecvd = 0;
						content.inputActionProcessed = 0;
						content.connectionRequestsProcessed = 0;
						content.updatesProcessed = 0;
						content.updateInfoEventsProcessed = 0;
						content.sentUpdateCount = 0;
						content.pendingEventCount = 0;
					}
				});
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
	public void onEvent(byte[] eventBytes, long sequence, long nextDeadline)
			throws Exception {
		if (!_incomingQueueHeader.isValid(eventBytes)) return;
		
		_metricContainer.getMetricEntry().eventsRecvd++;
		
		int recvSocketId = _incomingQueueHeader.getSocketId(eventBytes);
		int eventTypeId = EventPattern.getEventType(eventBytes, _incomingQueueHeader);
		
		if (recvSocketId == _routerSocketId) {
			final byte[] clientSocketId = RouterPattern.getSocketId(eventBytes, _incomingQueueHeader);
			if (eventTypeId == CLIENT_CONNECT.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _clientConnectEvent, new EventReader<IncomingEventHeader, ClientConnectEvent>() {

					@Override
					public void read(IncomingEventHeader header, ClientConnectEvent event) {
						onClientConnected(clientSocketId, event);
					}
					
				});				
			} else if (eventTypeId == CLIENT_INPUT.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _clientInputEvent, new EventReader<IncomingEventHeader, ClientInputEvent>() {

					@Override
					public void read(IncomingEventHeader header, ClientInputEvent event) {
						onClientInput(clientSocketId, event);
					}
				});
			} else {
				Log.warn(String.format("Unknown event type %d received on the router socket.", eventTypeId));
			}
		} else if (recvSocketId == _subSocketId) {
			if (eventTypeId == STATE_UPDATE.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _stateUpdateEvent, new EventReader<IncomingEventHeader, StateUpdateEvent>() {

					@Override
					public void read(IncomingEventHeader header, StateUpdateEvent event) {
						long updateId = event.getUpdateId();
						int contentIndex = _incomingQueueHeader.getSegmentCount() - 1;
						int contentMetaData = _incomingQueueHeader.getSegmentMetaData(event.getBackingArray(), contentIndex);
						int contentOffset = EventHeader.getSegmentOffset(contentMetaData);
						int contentLength = EventHeader.getSegmentLength(contentMetaData);
						_updateHandler.addUpdate(updateId, event.getBackingArray(), contentOffset, contentLength);
						if (_updateHandler.hasFullUpdateData(updateId)) {
							_updateHandler.sendUpdates(updateId, _clientLookup, _routerSendQueue);
						}
						_metricContainer.getMetricEntry().updatesProcessed++;
					}
					
				});				
			} else if (eventTypeId == STATE_INFO.getId()) {
				EventPattern.readContent(eventBytes, _incomingQueueHeader, _updateInfoEvent, new EventReader<IncomingEventHeader, StateUpdateInfoEvent>() {

					@Override
					public void read(IncomingEventHeader header, StateUpdateInfoEvent event) {
						onUpdateInfoEvent(event);
					}
					
				});	
			} else {
				Log.warn(String.format("Unknown event type %d received on the sub socket.", eventTypeId));
			}
		} else {
			Log.warn(String.format("Unknown socket ID: %d", recvSocketId));
		}
	}
	
	@Override
	public void onDeadline() {
		sendMetricEvents();
	}
	
	@Override
	public long moveToNextDeadline(long pendingEventCount) {
		_metricContainer.getMetricEntry().pendingEventCount = pendingEventCount;
		_nextMetricTime = _metricContainer.getMetricBucketEndTime(_lastSentMetricBucketId + 1);
		return _nextMetricTime;
	}
	
	@Override
	public long getDeadline() {
		return _nextMetricTime;
	}
	
	private void sendMetricEvents() {
		/*
		 * Update the last processed metric with the actual bucket IDs if
		 * they are present. Otherwise we just use the bucket ID that was
		 * current when this method was called. 
		 */
		_lastSentMetricBucketId = _metricContainer.getCurrentMetricBucketId();
		_metricContainer.forEachPending(new MetricLamda<MetricEntry>() {

			@Override
			public void call(final long bucketId, final MetricEntry metricEntry) {
				_metricSendQueue.send(PubSubPattern.asTask(_metricEvent, new EventWriter<OutgoingEventHeader, ClientHandlerMetricEvent>() {

					@Override
					public void write(OutgoingEventHeader header, ClientHandlerMetricEvent event) {
						event.setMetricBucketId(bucketId);
						event.setSourceId(_clientHandlerId);
						event.setActiveClientCount(_connectedClientCount);
						event.setBucketDuration(_metricContainer.getBucketDuration());
						event.setInputActionsProcessed(metricEntry.inputActionProcessed);
						event.setPendingEventCount(metricEntry.pendingEventCount);
						event.setTotalEventsProcessed(metricEntry.eventsRecvd);
						event.setUpdateEventsProcessed(metricEntry.updatesProcessed);
						event.setUpdateInfoEventsProcessed(metricEntry.updateInfoEventsProcessed);
						event.setSentUpdateCount(metricEntry.sentUpdateCount);
						event.setConnectionRequestsProcessed(metricEntry.connectionRequestsProcessed);
					}
					
				}));
				_lastSentMetricBucketId = bucketId;
			}
			
		});
	}
	
	private void onClientConnected(final byte[] clientSocketId, final ClientConnectEvent connectEvent) {
		if (_nextClientId >= _clientLookup.getLength()) {
			// over-subscribed - turn away
			_routerSendQueue.send(RouterPattern.asTask(clientSocketId, _connectResEvent, new EventWriter<OutgoingEventHeader, ConnectResponseEvent>() {

				@Override
				public void write(OutgoingEventHeader header, ConnectResponseEvent event) {
					event.setResponseCode(ConnectResponseEvent.RES_ERROR);
					event.setCallbackBits(connectEvent.getCallbackBits());
				}
				
			}));
		} else {
			final long newClientId = _nextClientId++;
			ClientProxy newClient = _clientLookup.get(newClientId);
			
			newClient.setLastMsgTime(_clock.currentMillis());
			newClient.setSocketId(clientSocketId);
			newClient.setIsActive(true);
			
			// send a connect response
			_routerSendQueue.send(RouterPattern.asTask(newClient.getSocketId(), _connectResEvent, new EventWriter<OutgoingEventHeader, ConnectResponseEvent>() {

				@Override
				public void write(OutgoingEventHeader header, ConnectResponseEvent event) {
					event.setClientId(new ClientId(_clientHandlerId, newClientId));
					event.setResponseCode(ConnectResponseEvent.RES_OK);
					event.setCallbackBits(connectEvent.getCallbackBits());
				}
				
			}));
			_connectedClientCount++;
		}
		_metricContainer.getMetricEntry().connectionRequestsProcessed++;
	}
	
	private void onClientInput(final byte[] clientSocketId, final ClientInputEvent inputEvent) {
		//TODO 
		final ClientProxy client = _clientLookup.get(inputEvent.getClientId().getClientId());
		if (client == null) {
			Log.warn(String.format("Unknown client ID %d", inputEvent.getClientId().getClientId()));
		} else {
			_pubSendQueue.send(PubSubPattern.asTask(_stateInputEvent, new EventWriter<OutgoingEventHeader, StateInputEvent>() {
	
				@Override
				public void write(OutgoingEventHeader header, StateInputEvent event) {
					long handlerInputId = _inputId++;
					event.setClientHandlerId(_clientHandlerId);
					event.setInputId(handlerInputId);
					int inputBytes = inputEvent.copyFromInputBytes(event.getBackingArray(), event.getInputOffset(), event.getAvailableInputBufferLength());
					event.setUsedLength(inputBytes);
					client.storeAssociation(inputEvent.getClientActionId(), handlerInputId);
				}
				
			}));
			_metricContainer.getMetricEntry().inputActionProcessed++;
		}
	}
	
	private void onUpdateInfoEvent(final StateUpdateInfoEvent updateInfoEvent) {
		long highestSeq = -1;
		boolean handlerFound = false;
		for (int i = 0; i < updateInfoEvent.getEntryCount() && !handlerFound; i++) {
			if (updateInfoEvent.getClientHandlerIdAtIndex(i) == _clientHandlerId) {
				handlerFound = true;
				highestSeq = updateInfoEvent.getHighestSequenceAtIndex(i);
			}
		}
		if (handlerFound) {
			long updateId = updateInfoEvent.getUpdateId();
			_updateHandler.addUpdateMetaData(updateId, highestSeq);
			if (_updateHandler.hasFullUpdateData(updateId)) {
				_updateHandler.sendUpdates(updateId, _clientLookup, _routerSendQueue);
				_metricContainer.getMetricEntry().sentUpdateCount++;
			}
		}
		_metricContainer.getMetricEntry().updateInfoEventsProcessed++;
	}

}
