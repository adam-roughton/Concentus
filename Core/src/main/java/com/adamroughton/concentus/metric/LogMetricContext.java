package com.adamroughton.concentus.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.util.RunningStats;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class LogMetricContext extends MetricContextBase {
	
	private final Clock _clock;
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final Disruptor<LogEvent> _pubQueue = new Disruptor<>(LogMetricContext.LogEvent.FACTORY, 2048, _executor, 
			ProducerType.MULTI, new BlockingWaitStrategy());
	private MetricWaitTimer _printTimer;
	
	@SuppressWarnings("unchecked")
	public LogMetricContext(long metricTickMillis, 
			long metricBufferMillis,
			Clock clock) {
		super(metricTickMillis, metricBufferMillis, clock);
		_clock = clock;
		_pubQueue.handleEventsWith(new LogMetricContext.LogEntryEventHandler(getMetricBucketInfo()));
	}

	@Override
	protected MetricPublisher<RunningStats> newStatsMetricPublisher(
			MetricMetaData metaData) {
		return new MetricPublisher<RunningStats>() {
			
			@Override
			public void publish(long bucketId, MetricMetaData metricMetaData,
					RunningStats metricValue) {
				pushEntry(bucketId, metricMetaData, String.format("%1$f (std = %2$f, n = %3$d, min = %4$f, max = %5$f)",
						metricValue.getMean(), 
						metricValue.getStandardDeviation(), 
						metricValue.getCount(), 
						metricValue.getMin(), 
						metricValue.getMax()));
			}
		};
	}

	@Override
	protected LongValueMetricPublisher newCountMetricPublisher(
			MetricMetaData metaData) {
		
		return new LongValueMetricPublisher() {
			
			@Override
			public void publish(long bucketId, MetricMetaData metricMetaData,
					Long metricValue) {
				publishDirect(bucketId, metricMetaData, metricValue);
			}
			
			@Override
			public void publishDirect(long bucketId, MetricMetaData metricMetaData,
					long metricValue) {
				String entry;
				MetricType type = metricMetaData.getMetricType();
				if (type == MetricType.THROUGHPUT) {
					double throughput = ((double) metricValue / (double) getMetricBucketInfo().getBucketDuration()) * 1000;
					entry = String.format("%f [%d]", throughput, metricValue);
				} else {
					entry = Long.toString(metricValue);
				}
				pushEntry(bucketId, metricMetaData, entry);
			}
		};
	}
	
	private void pushEntry(final long bucketId, final MetricMetaData metricMetaData, final String metricContent) {
		_pubQueue.publishEvent(new EventTranslator<LogEvent>() {
			
			@Override
			public void translateTo(LogEvent event, long sequence) {
				event.bucketId = bucketId;
				event.metaData = metricMetaData;
				event.metricContent = metricContent;
				event.isTimeoutEntry = false;
			}
		});
	}
	
	@Override
	public void start(int metricSourceId) {
		super.start(metricSourceId);
		_pubQueue.start();
		
		long waitTime = Constants.METRIC_TICK;
		MetricBucketInfo bucketInfo = getMetricBucketInfo();
		_printTimer = new MetricWaitTimer(_pubQueue, bucketInfo.getCurrentBucketId(), bucketInfo, _clock, waitTime);
		_executor.execute(_printTimer);
	}
	
	public void halt() {
		_printTimer.halt();
		_pubQueue.halt();
	}
	
	/*
	 * Inner classes
	 */

	private static class LogEvent {
		long bucketId;
		MetricMetaData metaData;
		String metricContent;
		boolean isTimeoutEntry;
		static EventFactory<LogEvent> FACTORY = new EventFactory<LogEvent>() {

			@Override
			public LogEvent newInstance() {
				return new LogEvent();
			}
			
		};
	}
	
	private static class MetricWaitTimer implements Runnable {

		private final AtomicBoolean _haltSignal = new AtomicBoolean(false);
		private final AtomicBoolean _isRunning = new AtomicBoolean(false);
		
		private final Disruptor<LogEvent> _pubQueue;
		private final MetricBucketInfo _bucketInfo;
		private final Clock _clock;
		private final long _waitMillis;
		private long _nextBucketId;
		
		public MetricWaitTimer(Disruptor<LogEvent> pubQueue, 
				long startBucketId, 
				MetricBucketInfo bucketInfo, 
				Clock clock, 
				long waitTime) {
			_pubQueue = Objects.requireNonNull(pubQueue);
			_bucketInfo = Objects.requireNonNull(bucketInfo);
			_nextBucketId = startBucketId;
			_clock = Objects.requireNonNull(clock);
			_waitMillis = waitTime;
		}
		
		@Override
		public void run() {
			if (!_isRunning.compareAndSet(false, true))
				throw new IllegalStateException("The print timer can only be started once.");
			while (!_haltSignal.get()) {
				long nextPubTime = _bucketInfo.getBucketEndTime(_nextBucketId) + _waitMillis;
				long timeRemaining;
				while ((timeRemaining = nextPubTime - _clock.currentMillis()) > 0) {
					try {
						Thread.sleep(timeRemaining);
					} catch (InterruptedException e) {
					}
				}
				_pubQueue.publishEvent(new EventTranslator<LogMetricContext.LogEvent>() {
	
					@Override
					public void translateTo(LogEvent event, long sequence) {
						event.isTimeoutEntry = true;
						event.bucketId = _nextBucketId;
						event.metricContent = null;
						event.metaData = null;
					}
				});
				_nextBucketId++;
			}
			_haltSignal.set(false);
			_isRunning.set(false);
		}
		
		public void halt() {
			_haltSignal.set(true);
		}
		
	}
	
	private static class LogEntryEventHandler implements EventHandler<LogEvent> {

		private static class LogEntry implements Comparable<LogEntry> {

			private final String _reference;
			private final String _metricName;
			private final String _metricContent;
			
			public LogEntry(String reference, String metricName, String metricContent) {
				_reference = Objects.requireNonNull(reference);
				_metricName = Objects.requireNonNull(metricName);
				_metricContent = Objects.requireNonNull(metricContent);
			}
			
			public String getReference() {
				return _reference;
			}

			public String getMetricName() {
				return _metricName;
			}

			public String getMetricContent() {
				return _metricContent;
			}

			@Override
			public int compareTo(LogEntry other) {
				int referenceComparison = this._reference.compareTo(other._reference);
				if (referenceComparison != 0) {
					return referenceComparison;
				} 
				int metricNameComparison = this._metricName.compareTo(other._metricName);
				if (metricNameComparison != 0) {
					return metricNameComparison;
				} 
				return this._metricContent.compareTo(other._metricContent);
			}
			
		}
		
		private final Long2ObjectMap<List<LogEntry>> _bucketEntries = new Long2ObjectArrayMap<>();
		private final MetricBucketInfo _bucketInfo;
		private long _lastTimedOutId = -1;
		
		public LogEntryEventHandler(MetricBucketInfo bucketInfo) {
			_bucketInfo = Objects.requireNonNull(bucketInfo);
		}
		
		@Override
		public void onEvent(LogEvent entry, long sequence, boolean endOfBatch)
				throws Exception {
			// we delay printing over some wait period to allow time for 
			// entries to be collected into one print
			if (entry.isTimeoutEntry) {
				printBucket(entry.bucketId);
				_lastTimedOutId = entry.bucketId;
			} else {
				long bucketId = entry.bucketId;
				MetricMetaData metaData = entry.metaData;
				String reference = metaData.getReference();
				String metricContent = entry.metricContent;
				
				List<LogEntry> entries;
				if (_bucketEntries.containsKey(bucketId)) {
					entries = _bucketEntries.get(bucketId);
				} else {
					entries = new ArrayList<>();
					_bucketEntries.put(bucketId, entries);
				}
				LogEntry logEntry = new LogEntry(reference, metaData.getMetricName(), metricContent);
				entries.add(logEntry);
				
				// if the metric is for a bucket that has already been waited on, print immediately
				if (bucketId < _lastTimedOutId) {
					printBucket(bucketId);
				}
			}			
		}
		
		private void printBucket(long bucketId) {
	 		List<LogEntry> entries = _bucketEntries.get(bucketId);
	 		if (entries != null) {
	 			Collections.sort(entries);
				StringBuilder stringBuilder = new StringBuilder();
		 		stringBuilder.append(String.format("Bucket %d, Timestamp: %d\n", bucketId, bucketId * _bucketInfo.getBucketDuration()));
		 		String lastReference = null;
		 		for (LogEntry logEntry : entries) {
		 			if (logEntry.getReference() != lastReference) {
		 				stringBuilder.append(String.format("    %s\n", logEntry.getReference()));
		 			}
		 			lastReference = logEntry.getReference();
		 			stringBuilder.append(String.format("        .%s: %s\n", logEntry.getMetricName(), logEntry.getMetricContent()));
		 		}
			 	Log.info(stringBuilder.toString());
			 	_bucketEntries.remove(bucketId);
	 		}
		}
		
	}
	
}
