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

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.util.RunningStats;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.LifecycleAware;

import static com.adamroughton.concentus.util.Util.millisUntil;

public class TrackingDeadlineBasedEventHandlerDecorator<T> implements DeadlineBasedEventHandler<T>, LifecycleAware {

	private final DeadlineBasedEventHandler<T> _decoratedHandler;
	private final Clock _clock;
	private final RunningStats _nanosInOnEvent = new RunningStats();
	private final RunningStats _nanosInOnDeadline = new RunningStats();
	private final RunningStats _nanosInMoveToNextDeadline = new RunningStats();
	private final RunningStats _deadlinePeriodStats = new RunningStats();
	
	private long lastTraceTime = 0;
	
	public TrackingDeadlineBasedEventHandlerDecorator(DeadlineBasedEventHandler<T> decoratedHandler, Clock clock) {
		_decoratedHandler = Objects.requireNonNull(decoratedHandler);
		_clock = Objects.requireNonNull(clock);
	}
	
	private void traceIfReady() {
		long now = _clock.currentMillis();
		if (now - lastTraceTime > 1000) {
			Log.info(String.format("DeadlineEventHandler (%s):", _decoratedHandler.toString()));
			traceStats("onEvent (nanos)", _nanosInOnEvent);			
			traceStats("onDeadline (nanos)", _nanosInOnDeadline);
			traceStats("onMoveToNextDeadline (nanos)", _nanosInMoveToNextDeadline);
			traceStats("requested deadline period (millis)", _deadlinePeriodStats);
			lastTraceTime = now;
		}
	}
	
	private void traceStats(String name, RunningStats stats) {
		Log.info(String.format("    %s", Util.statsToString(name, stats)));
		stats.reset();
	}
	
	@Override
	public void onEvent(T event, long sequence, boolean isEndOfBatch)
			throws Exception {
		long startTime = _clock.nanoTime();
		_decoratedHandler.onEvent(event, sequence, isEndOfBatch);
		_nanosInOnEvent.push(_clock.nanoTime() - startTime);
	}

	@Override
	public void onDeadline() {
		long startTime = _clock.nanoTime();
		_decoratedHandler.onDeadline();
		_nanosInOnDeadline.push(_clock.nanoTime() - startTime);
	}

	@Override
	public long moveToNextDeadline(long pendingCount) {
		long startTime = _clock.nanoTime();
		long deadline = _decoratedHandler.moveToNextDeadline(pendingCount);
		_nanosInMoveToNextDeadline.push(_clock.nanoTime() - startTime);
		_deadlinePeriodStats.push(millisUntil(deadline, _clock));
		traceIfReady();
		return deadline;
	}

	@Override
	public long getDeadline() {
		return _decoratedHandler.getDeadline();
	}

	@Override
	public void onStart() {
		if (_decoratedHandler instanceof LifecycleAware) {
			((LifecycleAware)_decoratedHandler).onStart();
		}
	}

	@Override
	public void onShutdown() {
		if (_decoratedHandler instanceof LifecycleAware) {
			((LifecycleAware)_decoratedHandler).onShutdown();
		}
	}

	@Override
	public String name() {
		return _decoratedHandler.name();
	}

}
