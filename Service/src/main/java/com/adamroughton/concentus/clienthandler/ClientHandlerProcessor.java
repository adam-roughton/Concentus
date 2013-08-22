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
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.events.ClientConnectEvent;
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
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.model.ClientId;
import com.esotericsoftware.minlog.Log;

import static com.adamroughton.concentus.messaging.events.EventType.*;

public class ClientHandlerProcessor<TBuffer extends ResizingBuffer> implements DeadlineBasedEventHandler<TBuffer> {
	
	private final Clock _clock;
	
	private final int _clientHandlerId;
	private final int _routerSocketId;
	private final int _subSocketId;
	
	private final SendQueue<OutgoingEventHeader, TBuffer> _pubSendQueue;
	private final SendQueue<OutgoingEventHeader, TBuffer> _routerSendQueue;
	
	private final UpdateHandler _updateHandler;
	private final IncomingEventHeader _routerRecvHeader;
	private final IncomingEventHeader _subRecvHeader;
	
	private long _inputId = 0;
	private long _nextClientId = 0;
	private long _nextHeartbeat = -1;
	
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
	
	private final MetricContext _metricContext;
	private final MetricGroup _metrics;
	private final CountMetric _incomingThroughputMetric;
	private final CountMetric _inputActionThroughputMetric;
	private final CountMetric _connectionRequestThroughputMetric;
	private final CountMetric _incomingUpdateThroughputMetric;
	private final CountMetric _incomingUpdateInfoThroughputMetric;
	private final CountMetric _updateSendThroughputMetric;
	private final CountMetric _activeClientCountMetric;
	
	private long _nextDeadline = 0;
	private boolean _sendHeartbeatFlag = false;

	public ClientHandlerProcessor(
			Clock clock,
			int clientHandlerId,
			int routerSocketId,
			int subSocketId,
			SendQueue<OutgoingEventHeader, TBuffer> routerSendQueue,
			SendQueue<OutgoingEventHeader, TBuffer> pubSendQueue,
			IncomingEventHeader routerRecvHeader,
			IncomingEventHeader subRecvHeader,
			MetricContext metricContext) {
		_clock = Objects.requireNonNull(clock);
		_clientHandlerId = clientHandlerId;
		_routerSocketId = routerSocketId;
		_subSocketId = subSocketId;
		
		_routerSendQueue = Objects.requireNonNull(routerSendQueue);
		_pubSendQueue = Objects.requireNonNull(pubSendQueue);
		
		_routerRecvHeader = routerRecvHeader;
		_subRecvHeader = subRecvHeader;
		
		_metricContext = Objects.requireNonNull(metricContext);
		_metrics = new MetricGroup();
		String reference = name();
		_incomingThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "incomingThroughput", false));
		_inputActionThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "inputActionThroughput", false));
		_connectionRequestThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "connectionRequestThroughput", false));
		_incomingUpdateThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "incomingUpdateThroughput", false));
		_incomingUpdateInfoThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "incomingUpdateInfoThroughput", false));
		_updateSendThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "updateSendThroughput", false));
		_activeClientCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "activeClientCount", true));
		
		_updateHandler = new UpdateHandler(32, reference, _metrics, _metricContext);
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
	public void onEvent(TBuffer eventBuffer, long sequence, boolean isEndOfBatch)
			throws Exception {
		if (!EventHeader.isValid(eventBuffer, 0)) {
			return;
		}		
		_incomingThroughputMetric.push(1);
		
		if (_routerRecvHeader.hasMyHeader(eventBuffer) && _routerRecvHeader.getSocketId(eventBuffer) == _routerSocketId) {
			int eventTypeId = EventPattern.getEventType(eventBuffer, _routerRecvHeader);
			final byte[] clientSocketId = RouterPattern.getSocketId(eventBuffer, _routerRecvHeader);
			if (eventTypeId == CLIENT_CONNECT.getId()) {
				EventPattern.readContent(eventBuffer, _routerRecvHeader, _clientConnectEvent, new EventReader<IncomingEventHeader, ClientConnectEvent>() {

					@Override
					public void read(IncomingEventHeader header, ClientConnectEvent event) {
						onClientConnected(clientSocketId, event);
					}
					
				});				
			} else if (eventTypeId == CLIENT_INPUT.getId()) {
				EventPattern.readContent(eventBuffer, _routerRecvHeader, _clientInputEvent, new EventReader<IncomingEventHeader, ClientInputEvent>() {

					@Override
					public void read(IncomingEventHeader header, ClientInputEvent event) {
						onClientInput(clientSocketId, event);
					}
				});
			} else {
				Log.warn(String.format("Unknown event type %d received on the router socket.", eventTypeId));
			}
		} else if (_subRecvHeader.hasMyHeader(eventBuffer) && _subRecvHeader.getSocketId(eventBuffer) == _subSocketId) {
			int eventTypeId = EventPattern.getEventType(eventBuffer, _subRecvHeader);
			if (eventTypeId == STATE_UPDATE.getId()) {
				EventPattern.readContent(eventBuffer, _subRecvHeader, _stateUpdateEvent, new EventReader<IncomingEventHeader, StateUpdateEvent>() {

					@Override
					public void read(IncomingEventHeader header, StateUpdateEvent event) {
						long updateId = event.getUpdateId();
						int contentIndex = _subRecvHeader.getSegmentCount() - 1;
						int contentMetaData = _subRecvHeader.getSegmentMetaData(event.getBuffer(), contentIndex);
						int contentOffset = EventHeader.getSegmentOffset(contentMetaData);
						int contentLength = EventHeader.getSegmentLength(contentMetaData);
						_updateHandler.addUpdate(updateId, event.getBuffer(), contentOffset, contentLength);
						_incomingUpdateThroughputMetric.push(1);
						if (_updateHandler.hasFullUpdateData(updateId)) {
							_updateHandler.sendUpdates(updateId, _clientLookup, _routerSendQueue);
						}
					}
					
				});				
			} else if (eventTypeId == STATE_INFO.getId()) {
				EventPattern.readContent(eventBuffer, _subRecvHeader, _updateInfoEvent, new EventReader<IncomingEventHeader, StateUpdateInfoEvent>() {

					@Override
					public void read(IncomingEventHeader header, StateUpdateInfoEvent event) {
						onUpdateInfoEvent(event);
					}
					
				});	
			} else {
				Log.warn(String.format("Unknown event type %d received on the sub socket.", eventTypeId));
			}
		} else {
			Log.warn(String.format("Unknown header ID: %d", EventHeader.getHeaderId(eventBuffer, 0)));
		}
	}
	
	@Override
	public void onDeadline() {
		if (_sendHeartbeatFlag) {
			sendHeartbeat();
			_sendHeartbeatFlag = false;
		} else {
			_metrics.publishPending();
		}
	}
	
	@Override
	public long moveToNextDeadline(long pendingEventCount) {
		long nextMetricDeadline = _metrics.nextBucketReadyTime();
		if (_nextHeartbeat < nextMetricDeadline) {
			_sendHeartbeatFlag = true;
			_nextDeadline = _nextHeartbeat;
		} else {
			_sendHeartbeatFlag = false;
			_nextDeadline = nextMetricDeadline;
		}
		return _nextDeadline;
	}
	
	@Override
	public long getDeadline() {
		return _nextDeadline;
	}
	
	private void onClientConnected(final byte[] clientSocketId, final ClientConnectEvent connectEvent) {
		if (_nextClientId >= _clientLookup.getLength()) {
			// over-subscribed - turn away
			_routerSendQueue.send(RouterPattern.asReliableTask(clientSocketId, _connectResEvent, new EventWriter<OutgoingEventHeader, ConnectResponseEvent>() {

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
			_routerSendQueue.send(RouterPattern.asReliableTask(newClient.getSocketId(), _connectResEvent, new EventWriter<OutgoingEventHeader, ConnectResponseEvent>() {

				@Override
				public void write(OutgoingEventHeader header, ConnectResponseEvent event) {
					event.setClientId(new ClientId(_clientHandlerId, newClientId));
					event.setResponseCode(ConnectResponseEvent.RES_OK);
					event.setCallbackBits(connectEvent.getCallbackBits());
				}
				
			}));
			_activeClientCountMetric.push(1);
		}
		_connectionRequestThroughputMetric.push(1);
	}
	
	private void onClientInput(final byte[] clientSocketId, final ClientInputEvent inputEvent) {
		//TODO
		try {
			final ClientProxy client = _clientLookup.get(inputEvent.getClientId().getClientId());
			_pubSendQueue.send(PubSubPattern.asTask(_stateInputEvent, new EventWriter<OutgoingEventHeader, StateInputEvent>() {
				
				@Override
				public void write(OutgoingEventHeader header, StateInputEvent event) {
					long handlerInputId = _inputId++;
					event.setClientHandlerId(_clientHandlerId);
					event.setInputId(handlerInputId);
					event.setIsHeartbeat(false);
					inputEvent.getInputSlice().copyTo(event.getInputSlice());
					client.storeAssociation(inputEvent.getClientActionId(), handlerInputId);
				}
				
			}));
			_nextHeartbeat = _clock.currentMillis() + Constants.METRIC_TICK;
			_inputActionThroughputMetric.push(1);
		} catch (ArrayIndexOutOfBoundsException eNotFound) {
			Log.warn(String.format("Unknown client ID %d", inputEvent.getClientId().getClientId()));
			Log.warn(String.format("Router Header: %d, Sub Header: %d", _routerRecvHeader.getHeaderId(), _subRecvHeader.getHeaderId()));
			Log.warn(inputEvent.getBuffer().toString());
		}
	}
	
	private void sendHeartbeat() {
		_pubSendQueue.send(PubSubPattern.asTask(_stateInputEvent, new EventWriter<OutgoingEventHeader, StateInputEvent>() {
			
			@Override
			public void write(OutgoingEventHeader header, StateInputEvent event) {
				long handlerInputId = _inputId++;
				event.setClientHandlerId(_clientHandlerId);
				event.setInputId(handlerInputId);
				event.setIsHeartbeat(true);
			}
			
		}));
		_nextHeartbeat = _clock.currentMillis() + Constants.METRIC_TICK;
	}
	
	private void onUpdateInfoEvent(final StateUpdateInfoEvent updateInfoEvent) {
		_incomingUpdateInfoThroughputMetric.push(1);
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
				_updateSendThroughputMetric.push(1);
			}
		}
	}

	@Override
	public String name() {
		return "clientHandlerProcessor";
	}

}
