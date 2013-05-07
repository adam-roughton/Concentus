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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.crowdhammer.messaging.events.TestEventType;
import com.adamroughton.concentus.crowdhammer.messaging.events.WorkerMetricEvent;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientHandlerMetricEvent;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.events.StateMetricEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.util.RunningStats;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.collections.Histogram;
import com.adamroughton.concentus.InitialiseDelegate;

public class MetricEventProcessor implements EventHandler<byte[]>, LifecycleAware {

	private final IncomingEventHeader _subHeader;
	
	private final StateMetricEvent _stateMetricEvent = new StateMetricEvent();
	private final WorkerMetricEvent _workerMetricEvent = new WorkerMetricEvent();
	private final ClientHandlerMetricEvent _clientHandlerMetricEvent = new ClientHandlerMetricEvent();
	
	private Histogram _histogram;
	private WorkerInfo[] _activeWorkers = new WorkerInfo[0];
	
	private long _clientCount;
	
	private boolean _isCollectingData = false;
	
	private BufferedWriter _latencyFileWriter;
	
	private static class WorkerMetricEntry {
		public long clientCount;
		public long workersXor;
		public RunningStats inputToUpdateLatency = null;
		public int lateInputResCount;
	}
	
	private StructuredSlidingWindowMap<WorkerMetricEntry> _workerMetrics;
	
	private RunningStats _canonicalStateThroughput = null;
	private RunningStats _inputToUpdateLatency = null;
	private int _lateInputUpdateCount = 0;
	
	public MetricEventProcessor(final IncomingEventHeader subHeader) {
		_subHeader = Objects.requireNonNull(subHeader);
		
		String baseName = "inputToUpdateLatency";
		Path path = Paths.get(baseName + ".csv");
		int i = 0;
		while (Files.exists(path)) {
			path = Paths.get(String.format("%s%d.csv", baseName, i++));
		}
		try {
			_latencyFileWriter = Files.newBufferedWriter(path, Charset.defaultCharset(), StandardOpenOption.CREATE_NEW);
			_latencyFileWriter.append(String.format("clients,mean,stddev,min,max,lateCount\n"));
			_latencyFileWriter.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void onEvent(byte[] eventBytes, long sequence, boolean endOfBatch)
			throws Exception {
		if (!_subHeader.isValid(eventBytes)) return;
		
		int eventType = EventPattern.getEventType(eventBytes, _subHeader);
		if (eventType == EventType.STATE_METRIC.getId()) {
			EventPattern.readContent(eventBytes, _subHeader, _stateMetricEvent, new EventReader<IncomingEventHeader, StateMetricEvent>() {

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
		} else if (eventType == EventType.CLIENT_HANDLER_METRIC.getId()) {
			EventPattern.readContent(eventBytes, _subHeader, _clientHandlerMetricEvent, new EventReader<IncomingEventHeader, ClientHandlerMetricEvent>() {

				@Override
				public void read(IncomingEventHeader header, ClientHandlerMetricEvent event) {
					processClientHandlerMetric(event);
				}
				
			});
		} else {
			Log.warn(String.format("Unrecognised event type %d", eventType));
		}
	}

	@Override
	public void onStart() {
		long[] upperBounds = new long[10000];
		for (int i = 0; i < 10000; i++) {
			upperBounds[i] = (i + 1) * 10;
		}
		_histogram = new Histogram(upperBounds);
		_isCollectingData = false;
		_canonicalStateThroughput = null;
		_inputToUpdateLatency = null;
	}

	@Override
	public void onShutdown() {
	}
	
	public void setActiveWorkers(WorkerInfo[] workers) {
		_activeWorkers = workers;
		long xor = 0;
		long clientCount = 0;
		for (WorkerInfo worker : workers) {
			xor ^= worker.getWorkerId();
			clientCount += worker.getSimClientCount();
		}
		final long workersXor = xor;
		_clientCount = clientCount;
		
		_workerMetrics = new StructuredSlidingWindowMap<WorkerMetricEntry>(128, 
				new ComponentFactory<WorkerMetricEntry>() {

			@Override
			public WorkerMetricEntry newInstance(Object[] initArgs) {
				return new WorkerMetricEntry();
			}
		}, new InitialiseDelegate<WorkerMetricEntry>() {

			@Override
			public void initialise(WorkerMetricEntry content) {
				content.clientCount = 0;
				content.workersXor = workersXor;
				content.lateInputResCount = 0;
				content.inputToUpdateLatency = null;
			}
			
		});
	}
	
	private void processStateMetric(final StateMetricEvent event) {
		long actionsProcessed = event.getInputActionsProcessed();
		long duration = event.getBucketDuration();
		
		double throughput = 0;
		if (duration > 0) {
			throughput = ((double) actionsProcessed / (double) duration) * 1000;
			_histogram.addObservation((long)throughput);
		}
		
		Log.info(String.format("StateMetric (B%d): %f actionsProc/s, %d pending events", event.getMetricBucketId(), throughput, event.getPendingEventCount()));
	}
	
	private void processWorkerMetric(final WorkerMetricEvent event) {
		long bucketId = event.getMetricBucketId();
		
		RunningStats inputToActionLatencyStats = new RunningStats(
				event.getInputToUpdateLatencyCount(),
				event.getInputToUpdateLatencyMean(),
				event.getInputToUpdateLatencySumSqrs(),
				event.getInputToUpdateLatencyMin(),
				event.getInputToUpdateLatencyMax());
		
		WorkerMetricEntry metricEntry = _workerMetrics.get(bucketId);
		metricEntry.clientCount += event.getConnectedClientCount();
		metricEntry.lateInputResCount += event.getInputToUpdateLatencyLateCount();
		if (metricEntry.inputToUpdateLatency == null) {
			metricEntry.inputToUpdateLatency = inputToActionLatencyStats;
		} else {
			metricEntry.inputToUpdateLatency.merge(inputToActionLatencyStats);
		}
		metricEntry.workersXor ^= event.getSourceId();
		if (metricEntry.workersXor == 0) {
			if (metricEntry.clientCount >= _clientCount) {
				_isCollectingData = true;
			}
			if (_isCollectingData) {
				if (_inputToUpdateLatency == null) {
					_inputToUpdateLatency = metricEntry.inputToUpdateLatency;
				} else {
					_inputToUpdateLatency.merge(metricEntry.inputToUpdateLatency);
				}
				_lateInputUpdateCount += metricEntry.lateInputResCount;
			}
		}
		
		long actionsSent = event.getSentInputActionsCount();
		long duration = event.getBucketDuration();
		double throughput = 0;
		if (duration > 0) {
			throughput = ((double) actionsSent / (double) duration) * 1000;
		}
		
		Log.info(String.format("WorkerMetric (B%d,%d): %f input/s, %d clients, %d errors, %d pending events, [%f mean, %f stddev, %f max, %f min] input to update latency", 
				event.getMetricBucketId(), 
				event.getSourceId(), 
				throughput, 
				event.getConnectedClientCount(), 
				event.getEventErrorCount(),
				event.getPendingEventCount(),
				inputToActionLatencyStats.getMean(),
				inputToActionLatencyStats.getStandardDeviation(),
				inputToActionLatencyStats.getMax(),
				inputToActionLatencyStats.getMin()));
	}
	
	private void processClientHandlerMetric(final ClientHandlerMetricEvent event) {
		long inputActionsProcessed = event.getInputActionsProcessed();
		long connectionRequestsProcesed = event.getConnectRequestsProcessed();
		long totalActionsProcessed = event.getTotalEventsProcessed();
		long updatesProcessed = event.getUpdateEventsProcessed();
		long updateInfoEventsProcessed = event.getUpdateInfoEventsProcessed();
		long updatesSent = event.getSentUpdateCount();
		
		long duration = event.getBucketDuration();
		
		double inputActionthroughput = 0;
		double totalActionthroughput = 0;
		double connectionRequestThroughput = 0;
		double updateRecvThroughput = 0;
		double updateInfoThroughput = 0;
		double updatesSentThroughput = 0;
		if (duration > 0) {
			inputActionthroughput = ((double) inputActionsProcessed / (double) duration) * 1000;
			_histogram.addObservation((long)inputActionthroughput);
			totalActionthroughput = ((double) totalActionsProcessed / (double) duration) * 1000;
			connectionRequestThroughput = ((double) connectionRequestsProcesed / (double) duration) * 1000;
			updateRecvThroughput = ((double) updatesProcessed / (double) duration) * 1000;
			updateInfoThroughput = ((double) updateInfoEventsProcessed / (double) duration) * 1000;
			updatesSentThroughput = ((double) updatesSent / (double) duration) * 1000;
		}
		
		Log.info(String.format("ClientHandlerMetric (B%d,%d): " +
				"%f action/s, " +
				"%f conn/s, " +
				"%f updatesProc/s, " +
				"%f updateInfo/s, " +
				"%f updateSent/s, " +
				"%f t.event/s, " +
				"%d pending", 
				event.getMetricBucketId(), 
				event.getSourceId(), 
				inputActionthroughput, 
				connectionRequestThroughput,
				updateRecvThroughput,
				updateInfoThroughput,
				updatesSentThroughput,
				totalActionthroughput, 
				event.getPendingEventCount()));
	}
	
	//TODO hack just to get the file for now
	public void endOfTest() {
		try {
			_latencyFileWriter.append(String.format("%d,%f,%f,%f,%f,%d\n", 
					_clientCount,
					_inputToUpdateLatency.getMean(), 
					_inputToUpdateLatency.getStandardDeviation(),
					_inputToUpdateLatency.getMin(),
					_inputToUpdateLatency.getMax(),
					_lateInputUpdateCount
					));
			_latencyFileWriter.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	// hack
	public void closeOpenFiles() {
		try {
			_latencyFileWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
