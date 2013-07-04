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
import java.util.concurrent.TimeUnit;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateEvent;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.RouterPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.metric.StatsMetric;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.adamroughton.concentus.util.SlidingWindowLongMap;
import com.adamroughton.concentus.util.Util;

class UpdateHandler {

	private final StructuredSlidingWindowMap<byte[]> _updateBuffer;
	private final SlidingWindowLongMap _updateToInputSeqMap;
	
	private final StateUpdateEvent _updateEvent = new StateUpdateEvent();
	private final ClientUpdateEvent _clientUpdateEvent = new ClientUpdateEvent();
	
	private final MetricContext _metricContext;
	private final StatsMetric _updateBroadcastMillisStatsMetric;
	private final StatsMetric _updateBroadcastMillisWaitingForSendStatsMetric;
	private final StatsMetric _updateBroadcastPercentWaitingForSendStatsMetric;
	private final StatsMetric _updateBroadcastMillisLookingUpHighestActionIdStatsMetric;
	private final StatsMetric _updateBroadcastPercentLookingUpHighestActionIdStatsMetric;
	
	public UpdateHandler(int bufferSize, String metricReference, MetricGroup metrics, MetricContext metricContext) {
		if (bufferSize < 0) 
			throw new IllegalArgumentException("The buffer size must be greater than 0.");
		_metricContext = Objects.requireNonNull(metricContext);		
		_updateBroadcastMillisStatsMetric = metrics.add(_metricContext.newStatsMetric(metricReference, "updateBroadcastMillisStats", false));
		_updateBroadcastMillisWaitingForSendStatsMetric = metrics.add(_metricContext.newStatsMetric(metricReference, "updateBroadcastMillisWaitingForSendStats", false));
		_updateBroadcastPercentWaitingForSendStatsMetric = metrics.add(_metricContext.newStatsMetric(metricReference, "updateBroadcastPercentWaitingForSendStats", false));
		_updateBroadcastMillisLookingUpHighestActionIdStatsMetric = metrics.add(
				_metricContext.newStatsMetric(metricReference, "updateBroadcastMillisLookingUpHighestActionIdStats", false));
		_updateBroadcastPercentLookingUpHighestActionIdStatsMetric = metrics.add(
				_metricContext.newStatsMetric(metricReference, "updateBroadcastPercentLookingUpHighestActionIdStats", false));
		
		_updateBuffer = new StructuredSlidingWindowMap<>(bufferSize, 
				byte[].class,
				new ComponentFactory<byte[]>() {

					@Override
					public byte[] newInstance(Object[] initArgs) {
						return new byte[Constants.MSG_BUFFER_ENTRY_LENGTH + 4];
					}
					
				}, new InitialiseDelegate<byte[]>() {
	
					@Override
					public void initialise(byte[] content) {
						/* 
						 * the first 4 bytes are reserved for the 
						 * event size
						 */
						MessageBytesUtil.writeInt(content, 0, 0);
					}
					
				});
		_updateToInputSeqMap = new SlidingWindowLongMap(bufferSize);
	}
	
	public void addUpdate(long updateId, byte[] eventBuffer, int eventOffset, int eventLength) {
		_updateBuffer.advanceTo(updateId);
		byte[] updateBufferEntry = _updateBuffer.get(updateId);
		int lengthToCopy = eventLength < updateBufferEntry.length? eventLength : updateBufferEntry.length;
		MessageBytesUtil.writeInt(updateBufferEntry, 0, lengthToCopy);
		System.arraycopy(eventBuffer, eventOffset, updateBufferEntry, 4, lengthToCopy);
	}
	
	public void addUpdateMetaData(long updateId, long highestSeqProcessed) {
		_updateToInputSeqMap.put(updateId, highestSeqProcessed);
	}
	
	public boolean hasFullUpdateData(long updateId) {
		return _updateBuffer.containsIndex(updateId) && _updateToInputSeqMap.containsIndex(updateId);
	}
	
	public void sendUpdates(final long updateId, final Iterable<ClientProxy> clients, final SendQueue<OutgoingEventHeader> updateQueue) {
		Clock clock = _metricContext.getClock();
		long broadcastStartTime = clock.nanoTime();
		long nanosWaitingForSend = 0;
		long nanosGettingHighestActionId = 0;
		
		final long highestHandlerSeq = _updateToInputSeqMap.getDirect(updateId);
		final byte[] updateBufferEntry = _updateBuffer.get(updateId);
		try {
			_updateEvent.setBackingArray(updateBufferEntry, 4);
			for (final ClientProxy client : clients) {
				if (client.isActive()) {
					final long nextUpdateId = client.getLastUpdateId() + 1;
					long lookupStart = clock.nanoTime();
					final long highestInputAction = client.lookupActionId(highestHandlerSeq);
					nanosGettingHighestActionId += clock.nanoTime() - lookupStart;
					
					long sendStartTime = clock.nanoTime();
					updateQueue.send(RouterPattern.asTask(client.getSocketId(), _clientUpdateEvent, new EventWriter<OutgoingEventHeader, ClientUpdateEvent>() {
		
						@Override
						public void write(OutgoingEventHeader header,
								ClientUpdateEvent event) throws Exception {
							event.setClientId(client.getClientId());
							event.setUpdateId(nextUpdateId);
							event.setSimTime(_updateEvent.getSimTime());
							event.setHighestInputActionId(highestInputAction);
							int copyLength = _updateEvent.copyUpdateBytes(event.getBackingArray(), 
									event.getUpdateOffset(), 
									event.getMaxUpdateBufferLength());
							event.setUsedLength(copyLength);
						}
						
					}));
					nanosWaitingForSend += clock.nanoTime() - sendStartTime;
				}
			}
		} finally {
			_updateEvent.releaseBackingArray();
		}
		long broadcastNanos = clock.nanoTime() - broadcastStartTime;
		_updateBroadcastMillisStatsMetric.push(TimeUnit.NANOSECONDS.toMillis(broadcastNanos));
		_updateBroadcastMillisWaitingForSendStatsMetric.push(TimeUnit.NANOSECONDS.toMillis(nanosWaitingForSend));
		_updateBroadcastPercentWaitingForSendStatsMetric.push(Util.getPercentage(nanosWaitingForSend, broadcastNanos));
		_updateBroadcastMillisLookingUpHighestActionIdStatsMetric.push(TimeUnit.NANOSECONDS.toMillis(nanosGettingHighestActionId));
		_updateBroadcastPercentLookingUpHighestActionIdStatsMetric.push(Util.getPercentage(nanosGettingHighestActionId, broadcastNanos));
	}
	
}
