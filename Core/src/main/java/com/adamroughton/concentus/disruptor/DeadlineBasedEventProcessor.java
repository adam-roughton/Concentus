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
package com.adamroughton.concentus.disruptor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.metric.MetricGroup;
import com.adamroughton.concentus.metric.StatsMetric;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import static com.adamroughton.concentus.util.Util.millisUntil;

public class DeadlineBasedEventProcessor<T> implements EventProcessor {

	private final AtomicBoolean _running = new AtomicBoolean(false);
	private final Sequence _sequence;
	private final Clock _clock;
	
	private final DeadlineBasedEventHandler<T> _eventHandler;
	private final DataProvider<T> _eventProvider;
	private final SequenceBarrier _barrier;
	private final FatalExceptionCallback _exceptionCallback;
	
	private final MetricContext _metricContext;
	private final MetricGroup _metrics;
	private final StatsMetric _pendingEventCountStatsMetric;
	private final CountMetric _onDeadlineInvocationThroughputMetric;
	private final CountMetric _moveToDeadlineInvocationThroughputMetric;
	
	public DeadlineBasedEventProcessor(
			MetricContext metricContext,
			Clock clock,
			DeadlineBasedEventHandler<T> eventHandler,
			DataProvider<T> eventProvider,
			SequenceBarrier barrier,
			FatalExceptionCallback exceptionCallback) {
		_clock = Objects.requireNonNull(clock);

		_eventHandler = Objects.requireNonNull(eventHandler);
		_eventProvider = Objects.requireNonNull(eventProvider);
		_barrier = Objects.requireNonNull(barrier);
		_exceptionCallback = Objects.requireNonNull(exceptionCallback);
		
		_sequence = new Sequence(-1);
		
		_metricContext = Objects.requireNonNull(metricContext);
		_metrics = new MetricGroup();
		String reference = String.format("DeadlineBasedProcessor[%s]", _eventHandler.name());
		_pendingEventCountStatsMetric = _metrics.add(_metricContext.newStatsMetric(reference, "pendingEventCountStats", false));
		_onDeadlineInvocationThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "onDeadlineInvocationThroughout", false));
		_moveToDeadlineInvocationThroughputMetric = _metrics.add(_metricContext.newThroughputMetric(reference, "moveToDeadlineInvocationThroughput", false));
	}
	
	@Override
	public void run() {
		if (!_running.compareAndSet(false, true)) {
			throw new IllegalStateException(String.format("The %s can only be started once.", 
					DeadlineBasedEventProcessor.class.getName()));
		}
		_barrier.clearAlert();
		
		if (_eventHandler instanceof LifecycleAware) {
			((LifecycleAware)_eventHandler).onStart();
		}
		
		long nextMetricTime = -1;
		long nextSequence = _sequence.get() + 1;
		long availableSeq = -1;
		while(true) {
			try {
				long pendingCount = _barrier.getCursor() - (nextSequence - 1);
				_pendingEventCountStatsMetric.push(pendingCount);
				long nextDeadline = _eventHandler.moveToNextDeadline(pendingCount);
				_moveToDeadlineInvocationThroughputMetric.push(1);
				
				// wait until the next sequence
				long remainingTime = millisUntil(nextDeadline, _clock);
				
				// process at least one event per deadline
				do {
					if (nextSequence <= availableSeq) {
						_eventHandler.onEvent(_eventProvider.get(nextSequence), nextSequence, nextSequence == availableSeq);
						 nextSequence++;
					} else {
						_sequence.set(nextSequence - 1);
						_barrier.checkAlert();
						remainingTime = millisUntil(nextDeadline, _clock);
						availableSeq = _barrier.getCursor(); //TODO: busy spin for now; perf with waitFor seems atrocious
						/* availableSeq = _barrier.waitFor(nextSequence, 
							remainingTime, TimeUnit.MILLISECONDS);*/
					}
				} while (remainingTime > 0);
				
				_eventHandler.onDeadline();
				_onDeadlineInvocationThroughputMetric.push(1);
				
				if (_clock.currentMillis() >= nextMetricTime) {
					_metrics.publishPending();
					nextMetricTime = _metrics.nextBucketReadyTime();
				}
				
			} catch (final AlertException eAlert) {
				if (!_running.get()) {
					break;
				}
			} catch (final Throwable e) {
				_exceptionCallback.signalFatalException(e);
			}
		}
		
		if (_eventHandler instanceof LifecycleAware) {
			((LifecycleAware)_eventHandler).onShutdown();
		}
	}
	
	@Override
	public Sequence getSequence() {
		return _sequence;
	}

	@Override
	public void halt() {
		_running.set(false);
		_barrier.alert();
	}

}
