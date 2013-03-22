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
package com.adamroughton.consentus.crowdhammer.metriclistener;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.messaging.events.EventType;
import com.adamroughton.consentus.messaging.events.StateMetricEvent;
import com.adamroughton.consentus.messaging.patterns.EventReader;
import com.adamroughton.consentus.messaging.patterns.SubRecvQueueReader;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.collections.Histogram;

public class MetricProcessor implements EventHandler<byte[]>, LifecycleAware {

	private final SubRecvQueueReader _subRecvQueueReader;
	private final StateMetricEvent _metricEvent = new StateMetricEvent();
	private long _lastUpdateId = -1;
	private Histogram _histogram;
	
	private long _lastPrintTime = -1;
	
	public MetricProcessor(final SubRecvQueueReader subRecvQueueReader) {
		_subRecvQueueReader = Objects.requireNonNull(subRecvQueueReader);
	}
	
	@Override
	public void onEvent(byte[] eventBytes, long sequence, boolean endOfBatch)
			throws Exception {
		int eventType = _subRecvQueueReader.getEventType(eventBytes);
		if (eventType == EventType.STATE_METRIC.getId()) {
			_subRecvQueueReader.read(eventBytes, _metricEvent, new EventReader<StateMetricEvent>() {

				@Override
				public void read(StateMetricEvent event) {
					processStateMetric(event);
				}
				
			});
		}
	}

	@Override
	public void onStart() {
		long[] upperBounds = new long[10000];
		for (int i = 0; i < 10000; i++) {
			upperBounds[i] = (i + 1) * 10;
		}
		_histogram = new Histogram(upperBounds);
	}

	@Override
	public void onShutdown() {
	}
	
	private void processStateMetric(final StateMetricEvent event) {
		long actionsProcessed = event.getInputActionsProcessed();
		long duration = event.getDurationInMs();
		
		double throughput = 0;
		if (duration > 0) {
			throughput = ((double) actionsProcessed / (double) duration) * 1000;
			_histogram.addObservation((long)throughput);
		}
		
		long missedEventCount = 0;
		long updateId = event.getUpdateId();
		if (updateId > _lastUpdateId + 1) {
			missedEventCount = updateId - _lastUpdateId;
		}
		_lastUpdateId = updateId;
		
		if (System.nanoTime() > _lastPrintTime + TimeUnit.SECONDS.toNanos(1)) {
			Log.info(String.format("Throughput: %f per second. Missed event count: %d", throughput, missedEventCount));
			
			_lastPrintTime = System.nanoTime();
		}
	}

}
