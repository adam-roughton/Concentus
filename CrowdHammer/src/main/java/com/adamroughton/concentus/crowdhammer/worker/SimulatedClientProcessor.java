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
package com.adamroughton.concentus.crowdhammer.worker;

import it.unimi.dsi.fastutil.longs.Long2LongArrayMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.metric.StatsMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.lmax.disruptor.LifecycleAware;

import uk.co.real_logic.intrinsics.StructuredArray;

public class SimulatedClientProcessor implements DeadlineBasedEventHandler<byte[]>, LifecycleAware {

	private final Long2LongMap _clientsIndex;
	private final StructuredArray<Client> _clients;
	private final long _activeClientCount;
	
	private volatile boolean _isSendingInput = false;
	
	private final SendQueue<OutgoingEventHeader> _clientSendQueue;
	private final IncomingEventHeader _recvHeader;
	
	private final ConnectResponseEvent _connectRes = new ConnectResponseEvent();
	private final ClientUpdateEvent _updateEvent = new ClientUpdateEvent();
	
	// Metrics
	private final MetricContext _metricContext;
	private final MetricGroup _metrics;
	private final CountMetric _connectedClientCountMetric;
	private final CountMetric _sentActionThroughputMetric;
	private final StatsMetric _actionEffectLatencyMetric;
	private final CountMetric _lateActionEffectCountMetric;	
	
	private long _nextClientIndex = -1;
	private boolean _sendMetric = false;
	private boolean _delayedClientDeadline = false;
	private long _nextDeadline = -1;
		
	public SimulatedClientProcessor(
			Clock clock,
			StructuredArray<Client> clients,
			long activeClientCount,
			SendQueue<OutgoingEventHeader> clientSendQueue,
			IncomingEventHeader recvHeader,
			MetricContext metricContext) {
		_clients = Objects.requireNonNull(clients);
		_activeClientCount = activeClientCount;
		
		// create an index for quickly looking up clients
		_clientsIndex = new Long2LongArrayMap((int)_clients.getLength());
		
		_clientSendQueue = Objects.requireNonNull(clientSendQueue);
		_recvHeader = Objects.requireNonNull(recvHeader);
		
		_metricContext = Objects.requireNonNull(metricContext);
		_metrics = new MetricGroup();
		String reference = name();
		_connectedClientCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "connectedClientCount", true));
		_sentActionThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "sentActionThroughput", false));
		_actionEffectLatencyMetric = _metrics.add(_metricContext.newStatsMetric(reference, "actionEffectLatency", false));
		_lateActionEffectCountMetric = _metrics.add(_metricContext.newCountMetric(reference, "lateActionEffectCount", false));
	}
	
	@Override
	public void onStart() {
		_isSendingInput = true;
	}

	@Override
	public void onShutdown() {
	}
	
	@Override
	public void onEvent(byte[] event, long sequence, boolean isEndOfBatch)
			throws Exception {
		if (!_recvHeader.isValid(event)) return;
		
		if (EventPattern.getEventType(event, _recvHeader) == EventType.CLIENT_UPDATE.getId()) {
			EventPattern.readContent(event, _recvHeader, _updateEvent, new EventReader<IncomingEventHeader, ClientUpdateEvent>() {

				@Override
				public void read(IncomingEventHeader header, ClientUpdateEvent event) {
					long clientId = event.getClientId();
					long clientIndex = _clientsIndex.get(clientId);
					Client updatedClient = _clients.get(clientIndex);
					updatedClient.onClientUpdate(event, _actionEffectLatencyMetric, _lateActionEffectCountMetric);
				}
			});
		} else if (EventPattern.getEventType(event, _recvHeader) == EventType.CONNECT_RES.getId()) {
			EventPattern.readContent(event, _recvHeader, _connectRes, new EventReader<IncomingEventHeader, ConnectResponseEvent>() {

				@Override
				public void read(IncomingEventHeader header, ConnectResponseEvent event) {
					// we use the index of the connecting client as the request ID
					long clientIndex = event.getCallbackBits();
					Client connectedClient = _clients.get(clientIndex);
					connectedClient.onConnectResponse(event, _connectedClientCountMetric);
					_clientsIndex.put(event.getClientIdBits(), clientIndex);
				}
			});
		}
	}

	@Override
	public void onDeadline() {
		if (_sendMetric) {
			_metrics.publishPending();
		} else if (_isSendingInput) {
			// only process the client if it will not block
			if (_clientSendQueue.isFull()) {
				_delayedClientDeadline = true;
			} else {
				Client client = _clients.get(_nextClientIndex);
				client.onActionDeadline(_clientSendQueue, _sentActionThroughputMetric);
			}
		}
	}

	@Override
	public long moveToNextDeadline(long pendingEventCount) {
		if (!_sendMetric && !_delayedClientDeadline) {
			// if we didn't send a metric on the last deadline, advance for the next client
			_nextClientIndex++;
			if (_nextClientIndex >= _activeClientCount) _nextClientIndex = 0;
		}
		
		long nextMetricDeadline = _metrics.nextBucketReadyTime();
		// FIX: stop the same client deadline being returned when we are no longer sending input
		long nextClientDeadline;
		if (_isSendingInput) {
			Client nextClient = _clients.get(_nextClientIndex);
			nextClientDeadline = nextClient.getNextDeadline();
		} else {
			nextClientDeadline = -1;
		}
		
		if (!_isSendingInput || nextMetricDeadline < nextClientDeadline) {
			_sendMetric = true;
			_nextDeadline = nextMetricDeadline;
		} else {
			_sendMetric = false;
			_delayedClientDeadline = false;
			_nextDeadline = nextClientDeadline;
		}
		return _nextDeadline;
	}

	@Override
	public long getDeadline() {
		return _nextDeadline;
	}
	
	public void stopSendingInput() {
		_isSendingInput = false;
	}

	@Override
	public String name() {
		return "clientProcessor";
	}
	
}
