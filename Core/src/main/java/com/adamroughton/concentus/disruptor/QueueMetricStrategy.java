package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.metric.StatsMetric;
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class QueueMetricStrategy<T> implements EventQueueStrategy<T> {

	private final MetricContext _metricContext;
	private final EventQueueStrategy<T> _decoratedStrategy;
	private final Clock _clock;
	private final EnqueueTimeCollector _enqueueTimeCollector;
	
	public QueueMetricStrategy(MetricContext metricContext, EventQueueStrategy<T> decoratedStrategy, Clock clock) {
		_metricContext = Objects.requireNonNull(metricContext);
		_decoratedStrategy = Objects.requireNonNull(decoratedStrategy);
		_clock = Objects.requireNonNull(clock);
		int queueLength = decoratedStrategy.getLength();
		_enqueueTimeCollector = new EnqueueTimeCollector(queueLength);
	}
	
	@Override
	public SequenceBarrier newBarrier(Sequence... sequencesToTrack) {
		return _decoratedStrategy.newBarrier(sequencesToTrack);
	}

	@Override
	public DataProvider<T> newQueueReader(final String readerName) {
		final DataProvider<T> decoratedDataProvider = _decoratedStrategy.newQueueReader(readerName);
		return new DataProvider<T>() {
			
			MetricGroup _metrics = new MetricGroup();
			String reference = String.format("QueueReader(%2$s->%1$s)", 
					readerName, _decoratedStrategy.getQueueName());
			StatsMetric _queuedTimeStats = _metrics.add(_metricContext.newStatsMetric(reference, "queueTimeStats", false));
			CountMetric _readThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "readThroughput", false));

			@Override
			public T get(long sequence) {
				long dequeueTime = _clock.currentMillis();
				long enqueueTime = _enqueueTimeCollector.getEnqueueTime(sequence);
				if (enqueueTime != -1) {
					long queuedTime = dequeueTime - enqueueTime;
					_enqueueTimeCollector.setEnqueueTime(sequence, -1);
					_queuedTimeStats.push(queuedTime);
				}
				_readThroughputMetric.push(1);
				this.emitMetricIfNeeded();
				return decoratedDataProvider.get(sequence);
			}
			
			private void emitMetricIfNeeded() {
				if (_clock.currentMillis() >= _metrics.nextBucketReadyTime()) {
					_metrics.publishPending();
				}
			}
		};
	}

	@Override
	public EventQueuePublisher<T> newQueuePublisher(final String publisherName, boolean isBlocking) {
		EventQueuePublisher<T> decoratedPublisher = _decoratedStrategy.newQueuePublisher(publisherName, isBlocking);
		return new MetricCollectingEventPublisher<>(_metricContext, _enqueueTimeCollector, _decoratedStrategy, _clock, decoratedPublisher);
	}

	@Override
	public long getCursor() {
		return _decoratedStrategy.getCursor();
	}

	@Override
	public int getLength() {
		return _decoratedStrategy.getLength();
	}
	
	@Override
	public long remainingCapacity() {
		return _decoratedStrategy.remainingCapacity();
	}

	@Override
	public void addGatingSequences(Sequence... sequences) {
		_decoratedStrategy.addGatingSequences(sequences);
	}

	@Override
	public boolean removeGatingSequence(Sequence sequence) {
		return _decoratedStrategy.removeGatingSequence(sequence);
	}

	@Override
	public String getQueueName() {
		return _decoratedStrategy.getQueueName();
	}

}
