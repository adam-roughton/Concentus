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
import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.MetricContainer;
import com.adamroughton.concentus.MetricContainer.MetricLamda;
import com.adamroughton.concentus.crowdhammer.messaging.events.WorkerMetricEvent;
import com.adamroughton.concentus.disruptor.DeadlineBasedEventHandler;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.PubSubPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.util.RunningStats;
import com.lmax.disruptor.LifecycleAware;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

public class SimulatedClientProcessor implements DeadlineBasedEventHandler<byte[]>, LifecycleAware {

	private final long _workerId;
	
	private final Long2LongMap _clientsIndex;
	private final StructuredArray<Client> _clients;
	private final long _activeClientCount;
	
	private volatile boolean _isSendingInput = false;
	
	private final SendQueue<OutgoingEventHeader> _clientSendQueue;
	private final SendQueue<OutgoingEventHeader> _metricSendQueue;	
	private final IncomingEventHeader _recvHeader;
	
	private final ConnectResponseEvent _connectRes = new ConnectResponseEvent();
	private final ClientUpdateEvent _updateEvent = new ClientUpdateEvent();
	private final WorkerMetricEvent _metricEvent = new WorkerMetricEvent();
	
	private final MetricContainer<WorkerMetrics> _metricContainer;
	private int _cummulativeConnectedClientCount = 0;
	
	private static class WorkerMetrics implements MetricEntry {
		public int connectedClientCount;
		public long sentActions;
		public long pendingEventCount;
		public RunningStats inputToUpdateLatencyStats = new RunningStats();
		public long lateInputToUpdateCount;
		
		@Override
		public void incrementConnectedClientCount(int amount) {
			connectedClientCount += amount;
		}
		
		@Override
		public void addInputToUpdateLatency(long latency) {
			inputToUpdateLatencyStats.push((double) latency);
		}
		
		@Override
		public void incrementInputToUpdateLateCount(int amount) {
			lateInputToUpdateCount += amount;
		}
		
		@Override
		public void incrementSentInputCount(int amount) {
			sentActions += amount;
		}
	}
	
	private long _nextClientIndex = -1;
	private long _lastProcessedMetricBucketId;
	private boolean _sendMetric = false;
	private long _nextDeadline = -1;
		
	public SimulatedClientProcessor(
			long workerId,
			Clock clock,
			StructuredArray<Client> clients,
			long activeClientCount,
			SendQueue<OutgoingEventHeader> clientSendQueue,
			SendQueue<OutgoingEventHeader> metricSendQueue,
			IncomingEventHeader recvHeader) {
		_workerId = workerId;
		_clients = Objects.requireNonNull(clients);
		_activeClientCount = activeClientCount;
		
		// create an index for quickly looking up clients
		_clientsIndex = new Long2LongArrayMap((int)_clients.getLength());
		
		_clientSendQueue = Objects.requireNonNull(clientSendQueue);
		_metricSendQueue = Objects.requireNonNull(metricSendQueue);
		_recvHeader = Objects.requireNonNull(recvHeader);
		
		_metricContainer = new MetricContainer<>(clock, 8,
			WorkerMetrics.class,
			new ComponentFactory<WorkerMetrics>() {
		
				@Override
				public WorkerMetrics newInstance(Object[] initArgs) {
					return new WorkerMetrics();
				}
			}, 
			new InitialiseDelegate<WorkerMetrics>(){
		
				@Override
				public void initialise(WorkerMetrics content) {
					content.connectedClientCount = 0;
					content.sentActions = 0;
					content.pendingEventCount = 0;
					content.inputToUpdateLatencyStats.reset();
					content.lateInputToUpdateCount = 0;
				}
				
			});
	}
	
	@Override
	public void onStart() {
		_isSendingInput = true;
		_cummulativeConnectedClientCount = 0;
		_lastProcessedMetricBucketId = -1;
	}

	@Override
	public void onShutdown() {
	}
	
	@Override
	public void onEvent(byte[] event, long sequence, long nextDeadline)
			throws Exception {
		if (!_recvHeader.isValid(event)) return;
		
		if (EventPattern.getEventType(event, _recvHeader) == EventType.CLIENT_UPDATE.getId()) {
			EventPattern.readContent(event, _recvHeader, _updateEvent, new EventReader<IncomingEventHeader, ClientUpdateEvent>() {

				@Override
				public void read(IncomingEventHeader header, ClientUpdateEvent event) {
					long clientId = event.getClientId();
					long clientIndex = _clientsIndex.get(clientId);
					Client updatedClient = _clients.get(clientIndex);
					updatedClient.onClientUpdate(event, _metricContainer.getMetricEntry());
				}
			});
		} else if (EventPattern.getEventType(event, _recvHeader) == EventType.CONNECT_RES.getId()) {
			EventPattern.readContent(event, _recvHeader, _connectRes, new EventReader<IncomingEventHeader, ConnectResponseEvent>() {

				@Override
				public void read(IncomingEventHeader header, ConnectResponseEvent event) {
					// we use the index of the connecting client as the request ID
					long clientIndex = event.getCallbackBits();
					Client connectedClient = _clients.get(clientIndex);
					connectedClient.onConnectResponse(event, _metricContainer.getMetricEntry());
					_clientsIndex.put(event.getClientIdBits(), clientIndex);
				}
			});
		}
	}

	@Override
	public void onDeadline() {
		if (_sendMetric) {
			sendMetricEvents();
		} else if (_isSendingInput) {
			Client client = _clients.get(_nextClientIndex);	
			client.onActionDeadline(_clientSendQueue, _metricContainer.getMetricEntry());
		}
	}

	@Override
	public long moveToNextDeadline(long pendingEventCount) {
		_metricContainer.getMetricEntry().pendingEventCount = pendingEventCount;
		if (!_sendMetric) {
			// if we didn't send a metric on the last deadline, advance for the next client
			_nextClientIndex++;
			if (_nextClientIndex >= _activeClientCount) _nextClientIndex = 0;
		}
		long nextMetricDeadline = _metricContainer.getMetricBucketEndTime(_lastProcessedMetricBucketId + 1);
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
	
	private void sendMetricEvents() {
		/*
		 * Update the last processed metric with the actual bucket IDs if
		 * they are present. Otherwise we just use the bucket ID that was
		 * current when this method was called. 
		 */
		_lastProcessedMetricBucketId = _metricContainer.getCurrentMetricBucketId();
		
		_metricContainer.forEachPending(new MetricLamda<WorkerMetrics>() {

			@Override
			public void call(final long bucketId, final WorkerMetrics metricEntry) {
				_cummulativeConnectedClientCount += metricEntry.connectedClientCount;
				_metricSendQueue.send(PubSubPattern.asTask(_metricEvent, new EventWriter<OutgoingEventHeader, WorkerMetricEvent>() {

					@Override
					public void write(OutgoingEventHeader header,
							WorkerMetricEvent event) throws Exception {
						event.setMetricBucketId(bucketId);
						event.setSourceId(_workerId);
						event.setBucketDuration(_metricContainer.getBucketDuration());
						event.setConnectedClientCount(_cummulativeConnectedClientCount);
						event.setSentInputActionsCount(metricEntry.sentActions);
						event.setPendingEventCount(metricEntry.pendingEventCount);
						// stats
						event.setInputToUpdateLatency(metricEntry.inputToUpdateLatencyStats);
						event.setInputToUpdateLatencyLateCount(metricEntry.lateInputToUpdateCount);
					}
					
				}));
				_lastProcessedMetricBucketId = bucketId;
			}
		});
	}
	
}
