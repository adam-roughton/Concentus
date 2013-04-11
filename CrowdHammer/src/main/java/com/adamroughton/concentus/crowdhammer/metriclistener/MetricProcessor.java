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
package com.adamroughton.concentus.crowdhammer.metriclistener;

import java.util.Objects;

import com.adamroughton.concentus.crowdhammer.messaging.events.TestEventType;
import com.adamroughton.concentus.crowdhammer.messaging.events.WorkerMetricEvent;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.events.StateMetricEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.collections.Histogram;

public class MetricProcessor implements EventHandler<byte[]>, LifecycleAware {

	private final IncomingEventHeader _subHeader;
	
	private final StateMetricEvent _metricEvent = new StateMetricEvent();
	private final WorkerMetricEvent _workerMetricEvent = new WorkerMetricEvent();
	
	private long _lastUpdateId = -1;
	private Histogram _histogram;
	
	public MetricProcessor(final IncomingEventHeader subHeader) {
		_subHeader = Objects.requireNonNull(subHeader);
	}
	
	@Override
	public void onEvent(byte[] eventBytes, long sequence, boolean endOfBatch)
			throws Exception {
		int eventType = EventPattern.getEventType(eventBytes, _subHeader);
		if (eventType == EventType.STATE_METRIC.getId()) {
			EventPattern.readContent(eventBytes, _subHeader, _metricEvent, new EventReader<IncomingEventHeader, StateMetricEvent>() {

				@Override
				public void read(IncomingEventHeader header, StateMetricEvent event) {
					processStateMetric(event);
				}
				
			});
		} else if (eventType == TestEventType.WORKER_METRIC.getId()) {
			EventPattern.readContent(eventBytes, _subHeader, _workerMetricEvent, new EventReader<IncomingEventHeader, WorkerMetricEvent>() {

				@Override
				public void read(IncomingEventHeader header, WorkerMetricEvent event) {
					processWorkerMetric(event);
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
		long duration = event.getActualBucketDurationInMs();
		
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
		
		Log.info(String.format("StateMetric (B%d): %f actionsProc/s, %d missed events", event.getMetricBucketId(), throughput, missedEventCount));
	}
	
	private void processWorkerMetric(final WorkerMetricEvent event) {
		long actionsSent = event.getSentInputActionsCount();
		long duration = event.getActualBucketDurationInMs();
		double throughput = 0;
		if (duration > 0) {
			throughput = ((double) actionsSent / (double) duration) * 1000;
		}
		Log.info(String.format("WorkerMetric (B%d,%d): %f input/s, %d errors", event.getMetricBucketId(), event.getWorkerId(), throughput, event.getEventErrorCount()));
	}

}
