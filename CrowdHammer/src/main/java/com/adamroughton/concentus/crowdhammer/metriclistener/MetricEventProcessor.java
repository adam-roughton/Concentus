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
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.events.MetricMetaDataEvent;
import com.adamroughton.concentus.messaging.events.MetricMetaDataRequestEvent;
import com.adamroughton.concentus.messaging.events.MetricEvent;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.metric.MetricType;
import com.adamroughton.concentus.util.RunningStats;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.collections.Histogram;
import com.adamroughton.concentus.InitialiseDelegate;

public class MetricEventProcessor implements EventHandler<byte[]>, LifecycleAware, Closeable {

	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(String.format("The driver for handling the sqlite database " +
					"for metric results was not found: '%s'", e.getMessage()), e);
		}
	}
	
	private final IncomingEventHeader _subHeader;
	
	private final MetricEvent _metricEvent = new MetricEvent();
	private final MetricMetaDataEvent _metricMetaDataEvent = new MetricMetaDataEvent();
	private final MetricMetaDataRequestEvent _metricMetaDataReqEvent = new MetricMetaDataRequestEvent();
	
	private boolean _isCollectingData = false;
	private Connection _connection;
	private BufferedWriter _latencyFileWriter;
	
	public MetricEventProcessor(IncomingEventHeader subHeader) {
		/*
		 * Establish a file that can be used for the database for these runs
		 */
		
		try {
			_connection = DriverManager.getConnection("jdbc:sqlite:(databasefile)");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
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
		if (eventType == EventType.METRIC.getId()) {
			EventPattern.readContent(eventBytes, _subHeader, _metricEvent, new EventReader<IncomingEventHeader, MetricEvent>() {

				@Override
				public void read(IncomingEventHeader header, MetricEvent event) {
					// TODO Auto-generated method stub
					
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
		_isCollectingData = false;
	}

	@Override
	public void onShutdown() {
	}
	
	private void processMetric(MetricEvent event) {
		int eventTypeId = event.getEventTypeId();
		StringBuilder logBuilder = new StringBuilder();
		logBuilder.append(String.format("StatsMetric (%d:%d): bucketId: %d, ",
				event.getSourceId(),
				event.getMetricId(),
				event.getMetricBucketId()));
		
		if (eventTypeId == MetricType.STATS.getId()) {
			RunningStats stats = MessageBytesUtil.readRunningStats(event.getBackingArray(), event.getMetricValueOffset());
			logBuilder.append(Util.statsToString("value", stats));
		} else if (eventTypeId == MetricType.COUNT.getId() || eventTypeId == MetricType.THROUGHPUT.getId()) {
			logBuilder.append(MessageBytesUtil.readInt(event.getBackingArray(), event.getMetricValueOffset()));
		} else {
			Log.warn(String.format("Unknown metric type %d", eventTypeId));
		}
		Log.info(logBuilder.toString());
		
		// find metric using metric ID
		// if metric not known, use source ID to request info
		// put into right table
	}
	
	//TODO hack just to get the file for now
	public void endOfTest() {
//		try {
//			if (_inputToUpdateLatency != null) {
//				_latencyFileWriter.append(String.format("%d,%f,%f,%f,%f,%d\n", 
//						_clientCount,
//						_inputToUpdateLatency.getMean(), 
//						_inputToUpdateLatency.getStandardDeviation(),
//						_inputToUpdateLatency.getMin(),
//						_inputToUpdateLatency.getMax(),
//						_lateInputUpdateCount
//						));
//			}
//			_latencyFileWriter.flush();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
	}
	
	// hack
	public void closeOpenFiles() {
		try {
			_latencyFileWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
