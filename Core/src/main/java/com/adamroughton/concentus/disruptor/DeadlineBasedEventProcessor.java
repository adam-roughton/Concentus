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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.util.RunningStats;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class DeadlineBasedEventProcessor<T> implements EventProcessor {

	private final AtomicBoolean _running = new AtomicBoolean(false);
	private final Sequence _sequence;
	private final Clock _clock;
	
	private final DeadlineBasedEventHandler<T> _eventHandler;
	private final RingBuffer<T> _ringBuffer;
	private final SequenceBarrier _barrier;
	private final FatalExceptionCallback _exceptionCallback;
	
	public DeadlineBasedEventProcessor(
			Clock clock,
			DeadlineBasedEventHandler<T> eventHandler,
			RingBuffer<T> ringBuffer,
			SequenceBarrier barrier,
			FatalExceptionCallback exceptionCallback) {
		_clock = Objects.requireNonNull(clock);
		_eventHandler = Objects.requireNonNull(eventHandler);
		_ringBuffer = Objects.requireNonNull(ringBuffer);
		_barrier = Objects.requireNonNull(barrier);
		_exceptionCallback = Objects.requireNonNull(exceptionCallback);
		
		_sequence = new Sequence(-1);
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
		
		//TODO REMOVE
		/*
		 * TESTING
		 */
		final RunningStats nanosInOnEvent = new RunningStats();
		final RunningStats nanosWaitingForEvents = new RunningStats();
		final RunningStats nanosInOnDeadline = new RunningStats();
		
		class TimingHelper {
			long lastTraceTime = 0;
			
			public void traceIfReady() {
				long now = _clock.currentMillis();
				if (now - lastTraceTime > 1000) {
					traceStats("onEvent (millis)", nanosInOnEvent);			
					traceStats("waitingForEvents (millis)", nanosWaitingForEvents);
					traceStats("onDeadline (millis)", nanosInOnDeadline);
					lastTraceTime = now;
				}
			}
			
			private void traceStats(String name, RunningStats stats) {
				Log.info(String.format("%s: %d invocations, %f avg, %f stdDev, %f max, %f min",
						name,
						stats.getCount(), 
						stats.getMean(), 
						stats.getStandardDeviation(), 
						stats.getMax(), 
						stats.getMin()));
			}
		}
		TimingHelper timingHelper = new TimingHelper();
		/*
		 * END TESTING
		 */
		
		long nextSequence = _sequence.get() + 1;
		while(true) {
			try {
				//_barrier.checkAlert();
				
				long pendingCount = _ringBuffer.getCursor() - (nextSequence - 1);
				long nextDeadline = _eventHandler.moveToNextDeadline(pendingCount);
				
				// wait until the next sequence
				long remainingTime = 0;
				long availableSequence = -1;
				
				
//				do {
//					if (nextSequence <= availableSequence) {
//						_barrier.checkAlert();
//						_eventHandler.onEvent(_ringBuffer.get(nextSequence), nextSequence, nextDeadline);
//						 nextSequence++;
//					} else {
//						_sequence.set(nextSequence - 1);
//						remainingTime = timeUntil(nextDeadline);
//						availableSequence = _barrier.waitFor(nextSequence, 
//								remainingTime, TimeUnit.MILLISECONDS);
//					}
//				} while (remainingTime > 0);
				
				
				while ((remainingTime = timeUntil(nextDeadline)) > 0) {
					if (nextSequence <= availableSequence) {
						_barrier.checkAlert();
						
						long startTime = _clock.nanoTime();
						_eventHandler.onEvent(_ringBuffer.get(nextSequence), nextSequence, nextDeadline);
						nanosInOnEvent.push(TimeUnit.NANOSECONDS.toMillis(_clock.nanoTime() - startTime));
						
						 nextSequence++;
					} else {
						_sequence.set(nextSequence - 1);
						long startTime = _clock.nanoTime();
						availableSequence = _barrier.waitFor(nextSequence, 
								remainingTime, TimeUnit.MILLISECONDS);
						nanosWaitingForEvents.push(TimeUnit.NANOSECONDS.toMillis(_clock.nanoTime() - startTime));
					}
				}
				
				long startTime = _clock.nanoTime();
				_eventHandler.onDeadline();
				nanosInOnDeadline.push(TimeUnit.NANOSECONDS.toMillis(_clock.nanoTime() - startTime));
				timingHelper.traceIfReady();
				
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

	private long timeUntil(long deadline) {
		long remainingTime = deadline - _clock.currentMillis();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
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
