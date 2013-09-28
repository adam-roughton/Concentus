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

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

import java.util.Objects;

import org.javatuples.Pair;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionReceiptEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientConnectEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientDisconnectedEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientHandlerInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientUpdateEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ConnectResponseEvent;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.RouterPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.esotericsoftware.minlog.Log;

import static com.adamroughton.concentus.data.DataType.*;

public class ClientHandlerProcessor<TBuffer extends ResizingBuffer> implements DeadlineBasedEventHandler<TBuffer> {
	
	private final Clock _clock;
	
	private final ActionCollectorAllocationStrategy _actionProcessorAllocator;
	private final int _clientHandlerId;
	private final int _routerSocketId;
	private final int _subSocketId;
	
	private final SendQueue<OutgoingEventHeader, TBuffer> _routerSendQueue;
	
	private final IncomingEventHeader _routerRecvHeader;
	private final IncomingEventHeader _subRecvHeader;
	
	private long _nextClientIndex = 0;
	
	private final StructuredArray<ClientProxy> _clientLookup;
	private final Int2LongMap _actionCollectorToReliableSeqLookup = new Int2LongOpenHashMap();
	{
		_actionCollectorToReliableSeqLookup.defaultReturnValue(-1);
	}
	
	private final ClientHandlerInputEvent _clientHandlerInputEvent = new ClientHandlerInputEvent();
	private final ActionReceiptEvent _actionReceiptEvent = new ActionReceiptEvent();
	private final ConnectResponseEvent _connectResEvent = new ConnectResponseEvent();
	private final ClientConnectEvent _clientConnectEvent = new ClientConnectEvent();
	private final ClientInputEvent _clientInputEvent = new ClientInputEvent();
	private final CanonicalStateUpdate _canonicalStateUpdate = new CanonicalStateUpdate();
	private final ClientUpdateEvent _clientUpdateEvent = new ClientUpdateEvent();
	private final ClientDisconnectedEvent _clientDisconnectedEvent = new ClientDisconnectedEvent();
	
	private final CanonicalStateUpdate _latestCanonicalState = new CanonicalStateUpdate();
	private final ArrayBackedResizingBuffer _latestCanonicalStateBuffer;
	
	private final MetricContext _metricContext;
	private final MetricGroup _metrics;
	private final CountMetric _incomingThroughputMetric;
	private final CountMetric _inputActionThroughputMetric;
	private final CountMetric _connectionRequestThroughputMetric;
	private final CountMetric _incomingUpdateThroughputMetric;
	private final CountMetric _updateSendThroughputMetric;
	private final CountMetric _activeClientCountMetric;
	private final CountMetric _droppedClientCountMetric;
	
	private long _nextDeadline = 0;

	public ClientHandlerProcessor(
			ActionCollectorAllocationStrategy actionProcessorAllocator,
			Clock clock,
			final int clientHandlerId,
			int routerSocketId,
			int subSocketId,
			SendQueue<OutgoingEventHeader, TBuffer> routerSendQueue,
			IncomingEventHeader routerRecvHeader,
			IncomingEventHeader subRecvHeader,
			MetricContext metricContext) {
		_clock = Objects.requireNonNull(clock);
		_actionProcessorAllocator = Objects.requireNonNull(actionProcessorAllocator);
		_clientHandlerId = clientHandlerId;
		_routerSocketId = routerSocketId;
		_subSocketId = subSocketId;
		
		_routerSendQueue = Objects.requireNonNull(routerSendQueue);
		
		_routerRecvHeader = routerRecvHeader;
		_subRecvHeader = subRecvHeader;
		
		_clientLookup = StructuredArray.newInstance(32 * 1024, ClientProxy.class, new ComponentFactory<ClientProxy>() {

			private long nextClientId = 0;
			
			@Override
			public ClientProxy newInstance(Object[] initArgs) {
				return new ClientProxy(new ClientId(clientHandlerId,  nextClientId++));
			}
		});
		
		_metricContext = Objects.requireNonNull(metricContext);
		_metrics = new MetricGroup();
		String reference = name();
		_incomingThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "incomingThroughput", false));
		_inputActionThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "inputActionThroughput", false));
		_connectionRequestThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "connectionRequestThroughput", false));
		_incomingUpdateThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "incomingUpdateThroughput", false));
		_updateSendThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "updateSendThroughput", false));
		_activeClientCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "activeClientCount", true));
		_droppedClientCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "droppedClientCount", true));
		
		/*
		 * Create canonical state buffer
		 * Initialise with values signalling no update
		 */
		_latestCanonicalStateBuffer = 
				new ArrayBackedResizingBuffer(Constants.DEFAULT_MSG_BUFFER_SIZE);
		_latestCanonicalState.attachToBuffer(_latestCanonicalStateBuffer);
		_latestCanonicalState.setUpdateId(-1);
		_latestCanonicalState.setTime(-1);
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
			if (_routerRecvHeader.isMessagingEvent(eventBuffer)) {
				_routerSendQueue.send(eventBuffer, _routerRecvHeader);
				return;
			}
			
			int eventTypeId = EventPattern.getEventType(eventBuffer, _routerRecvHeader);
			final SocketIdentity senderRef = RouterPattern.getSocketId(eventBuffer, _routerRecvHeader);
			if (eventTypeId == CLIENT_CONNECT_EVENT.getId()) {
				EventPattern.readContent(eventBuffer, _routerRecvHeader, _clientConnectEvent, new EventReader<IncomingEventHeader, ClientConnectEvent>() {

					@Override
					public void read(IncomingEventHeader header, ClientConnectEvent event) {
						onClientConnected(senderRef, event);
					}
					
				});				
			} else if (eventTypeId == CLIENT_INPUT_EVENT.getId()) {
				EventPattern.readContent(eventBuffer, _routerRecvHeader, _clientInputEvent, new EventReader<IncomingEventHeader, ClientInputEvent>() {

					@Override
					public void read(IncomingEventHeader header, ClientInputEvent event) {
						onClientInput(senderRef, event);
					}
				});
			} else if (eventTypeId == ACTION_RECEIPT_EVENT.getId()) {
				EventPattern.readContent(eventBuffer, _routerRecvHeader, _actionReceiptEvent, new EventReader<IncomingEventHeader, ActionReceiptEvent>() {

					@Override
					public void read(IncomingEventHeader header, ActionReceiptEvent event) {
						onActionReceipt(senderRef, event);
					}
				});
			} else {
				Log.warn(String.format("Unknown event type %d received on the router socket.", eventTypeId));
			}
		} else if (_subRecvHeader.hasMyHeader(eventBuffer) && _subRecvHeader.getSocketId(eventBuffer) == _subSocketId) {
			int eventTypeId = EventPattern.getEventType(eventBuffer, _subRecvHeader);
			if (eventTypeId == CANONICAL_STATE_UPDATE.getId()) {
				EventPattern.readContent(eventBuffer, _subRecvHeader, _canonicalStateUpdate, new EventReader<IncomingEventHeader, CanonicalStateUpdate>() {

					@Override
					public void read(IncomingEventHeader header, CanonicalStateUpdate canonicalStateUpdate) {
						onCanonicalStateUpdate(canonicalStateUpdate);
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
		_metrics.publishPending();
	}
	
	@Override
	public long moveToNextDeadline(long pendingEventCount) {
		_nextDeadline = _metrics.nextBucketReadyTime();
		return _nextDeadline;
	}
	
	@Override
	public long getDeadline() {
		return _nextDeadline;
	}
	
	private void onClientConnected(final SocketIdentity clientSocketId, final ClientConnectEvent connectEvent) {
		if (_nextClientIndex >= _clientLookup.getLength()) {
			// over-subscribed - turn away
			_routerSendQueue.send(RouterPattern.asReliableTask(clientSocketId, _connectResEvent, new EventWriter<OutgoingEventHeader, ConnectResponseEvent>() {

				@Override
				public void write(OutgoingEventHeader header, ConnectResponseEvent event) {
					event.setResponseCode(ConnectResponseEvent.RES_ERROR);
					event.setCallbackBits(connectEvent.getCallbackBits());
				}
				
			}));
		} else {
			final long newClientIndex = _nextClientIndex++;
			ClientProxy newClient = _clientLookup.get(newClientIndex);
			
			newClient.setLastMsgTime(_clock.currentMillis());
			newClient.setClientRef(clientSocketId);
			
			Pair<Integer, SocketIdentity> actionCollectorAllocation = _actionProcessorAllocator.allocateClient(newClientIndex);
			newClient.setActionCollectorAllocation(actionCollectorAllocation);
			
			newClient.setIsActive(true);
			
			// send a connect response
			_routerSendQueue.send(RouterPattern.asReliableTask(newClient.getClientSocketId(), 
					_connectResEvent, new EventWriter<OutgoingEventHeader, ConnectResponseEvent>() {

				@Override
				public void write(OutgoingEventHeader header, ConnectResponseEvent event) {
					event.setClientId(new ClientId(_clientHandlerId, newClientIndex));
					event.setResponseCode(ConnectResponseEvent.RES_OK);
					event.setCallbackBits(connectEvent.getCallbackBits());
				}
				
			}));
			_activeClientCountMetric.push(1);
		}
		_connectionRequestThroughputMetric.push(1);
	}
	
	private void onClientInput(final SocketIdentity senderRef, final ClientInputEvent inputEvent) {
		try {
			final ClientProxy client = _clientLookup.get(inputEvent.getClientId().getClientIndex());
			if (client.isActive()) {
				
				/*
				 * Send any action
				 */
				if (inputEvent.hasAction()) {
					int actionCollectorId = client.getActionCollectorId();
					final long reliableSeqAck = _actionCollectorToReliableSeqLookup.get(actionCollectorId);

					_routerSendQueue.send(RouterPattern.asReliableTask(client.getActionCollectorRef(), _clientHandlerInputEvent, 
							new EventWriter<OutgoingEventHeader, ClientHandlerInputEvent>() {
	
								@Override
								public void write(OutgoingEventHeader header,
										ClientHandlerInputEvent event) throws Exception {
									event.setClientHandlerId(_clientHandlerId);
									event.setReliableSeqAck(reliableSeqAck);
									event.setHasAction(true);
									inputEvent.getActionSlice().copyTo(event.getActionSlice());
								}
					}));
				}
				
				/*
				 * Generate an update for the client
				 */
				_routerSendQueue.send(RouterPattern.asReliableTask(client.getClientSocketId(), _clientUpdateEvent, 
						new EventWriter<OutgoingEventHeader, ClientUpdateEvent>(){

							@Override
							public void write(OutgoingEventHeader header,
									ClientUpdateEvent event) throws Exception {
								event.setClientId(client.getClientIdBits());
								client.generateUpdate(inputEvent, _latestCanonicalState, event);	
							}
				}));
				if (client.shouldDrop()) {
					client.clear();
					_droppedClientCountMetric.push(1);
					_routerSendQueue.send(RouterPattern.asReliableTask(senderRef, _clientDisconnectedEvent, 
							new EventWriter<OutgoingEventHeader, ClientDisconnectedEvent>() {

								@Override
								public void write(OutgoingEventHeader header,
										ClientDisconnectedEvent event)
										throws Exception {
									event.setClientId(inputEvent.getClientId());
								}
					}));
				} else {
					_updateSendThroughputMetric.push(1);
				}
			}
			_inputActionThroughputMetric.push(1);
		} catch (ArrayIndexOutOfBoundsException eNotFound) {
			Log.warn(String.format("Unknown client ID %d", inputEvent.getClientId().getClientIndex()));
			Log.warn(String.format("Router Header: %d, Sub Header: %d", _routerRecvHeader.getHeaderId(), _subRecvHeader.getHeaderId()));
			Log.warn(inputEvent.getBuffer().toString());
		}
	}
	
	private void onActionReceipt(SocketIdentity senderRef, ActionReceiptEvent receiptEvent) {
		int actionCollectorId = receiptEvent.getActionCollectorId();
		long reliableSeq = receiptEvent.getReliableSeq();
		long lastKnownReliableSeq = _actionCollectorToReliableSeqLookup.get(actionCollectorId);
		if (reliableSeq > lastKnownReliableSeq) {
			_actionCollectorToReliableSeqLookup.put(actionCollectorId, reliableSeq);
		}
		
		try {
			final ClientProxy client = _clientLookup.get(receiptEvent.getClientId().getClientIndex());
			if (client.isActive()) {
				client.processActionReceipt(receiptEvent);
			}
		} catch (ArrayIndexOutOfBoundsException eNotFound) {
			Log.warn(String.format("Unknown client ID %d", receiptEvent.getClientId().getClientIndex()));
			Log.warn(String.format("Router Header: %d, Sub Header: %d", _routerRecvHeader.getHeaderId(), _subRecvHeader.getHeaderId()));
			Log.warn(receiptEvent.getBuffer().toString());
		}
	}
	
	private void onCanonicalStateUpdate(CanonicalStateUpdate canonicalStateUpdateEvent) {
		if (_latestCanonicalState.getUpdateId() < canonicalStateUpdateEvent.getUpdateId()) {
			canonicalStateUpdateEvent.getBuffer().copyTo(_latestCanonicalStateBuffer);
		}
		_incomingUpdateThroughputMetric.push(1);
	}

	@Override
	public String name() {
		return "clientHandlerProcessor";
	}

}
