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

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.metric.NullMetricContext;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.YieldingWaitStrategy;

public class DeadlineBasedEventProcessorPerfTest {

	private DeadlineBasedEventProcessor<byte[]> _deadlineProcessor;
	private TestDeadlineEventHandler _testEventHandler;
	private RingBuffer<byte[]> _ringBuffer;
	private Thread _testThread;
	private long _eventsPerTick;
	private long _eventsPerTickCorrection;
	
	private static final long EVENT_RECV_TICK_PERIOD_MILLIS = 10;
	
	long deadlinePeriod;
	long onEventProcNanos;
	long deadlineProcNanos;
	long recvEventsPerSec;
	
	public void setUp() {
		Clock clock = new DefaultClock();
		
		_ringBuffer = RingBuffer.createSingleProducer(Util.msgBufferFactory(512), 1, new YieldingWaitStrategy());
		_ringBuffer.addGatingSequences(new Sequence(Long.MAX_VALUE));
		
		SequenceBarrier barrier = _ringBuffer.newBarrier();
		
		FatalExceptionCallback exceptionCallback = new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
			}
		};
		
		_testEventHandler = new TestDeadlineEventHandler(clock, deadlinePeriod, onEventProcNanos, deadlineProcNanos);
		_deadlineProcessor = new DeadlineBasedEventProcessor<>(new NullMetricContext(), clock, _testEventHandler, _ringBuffer, barrier, exceptionCallback);
		_testThread = new Thread(_deadlineProcessor);
		
		_eventsPerTick = recvEventsPerSec * EVENT_RECV_TICK_PERIOD_MILLIS / 1000;
		_eventsPerTickCorrection = recvEventsPerSec % 1000;
	}
	
	public long doTest(long runTimeMillis) throws InterruptedException {
		if (runTimeMillis == 0) return 0;
		
		long startTime = System.currentTimeMillis();
		long deadline = startTime + runTimeMillis;
		_testThread.start();
		long currentTime;
		long lastTickTime = startTime;
		long lastCorrectionTime = startTime;
		do {
			currentTime = System.currentTimeMillis();
			if ((currentTime - lastTickTime) / EVENT_RECV_TICK_PERIOD_MILLIS > 0) {
				long elapsedTicks = (currentTime - lastTickTime) / EVENT_RECV_TICK_PERIOD_MILLIS;
				lastTickTime = currentTime;
				long eventsToAdd = elapsedTicks * _eventsPerTick;
				long correctionTicksSinceLastCorrection = (currentTime - lastCorrectionTime) / 1000;
				if (correctionTicksSinceLastCorrection > 0) {
					eventsToAdd += correctionTicksSinceLastCorrection * _eventsPerTickCorrection;
					lastCorrectionTime = currentTime;
				}
				if (eventsToAdd > 0) {
					long currSeq = _ringBuffer.getCursor();
					_ringBuffer.publish(currSeq + eventsToAdd);
				}
			}
		} while (currentTime < deadline);
		_deadlineProcessor.halt();
		_testThread.join();
		return System.currentTimeMillis() - startTime;
	}
	
	public long onEventCallCount() {
		return _testEventHandler.onEventCallCount();
	}
	
	public long onDeadlineCallCount() {
		return _testEventHandler.onDeadlineCallCount();
	}
	
	public long getTestVal() {
		return _testEventHandler.getTestVal();
	}
	
	public static void main(String[] args) throws InterruptedException {
		long testPeriod = 1000;
		
		while (true) {
			DeadlineBasedEventProcessorPerfTest perfTest = new DeadlineBasedEventProcessorPerfTest();
			perfTest.deadlinePeriod = 100;
			perfTest.deadlineProcNanos = 1000;
			perfTest.onEventProcNanos = 1000;
			perfTest.recvEventsPerSec = 10000000;
			perfTest.setUp();
			long duration = perfTest.doTest(testPeriod);
			
			double onEventThroughput = (double) perfTest.onEventCallCount() / duration * TimeUnit.SECONDS.toMillis(1);
			double onDeadlineThroughput = (double) perfTest.onDeadlineCallCount() / duration * TimeUnit.SECONDS.toMillis(1);
			
			System.out.println(String.format("[%f events; %f deadlines]/s (testVal = %d)", onEventThroughput, onDeadlineThroughput, perfTest.getTestVal()));
		}
	}
	
	private static class TestDeadlineEventHandler implements DeadlineBasedEventHandler<byte[]> {
		
		private final Clock _clock;
		private final long _deadlinePeriod;
		private final long _onEventProcNanos;
		private final long _deadlineProcNanos;
		
		private long _nextDeadline = -1;
		private long _deadlineCallCount = 0;
		private long _onEventCallCount = 0;
		private long _val = 0;
		
		public TestDeadlineEventHandler(Clock clock, long deadlinePeriod, long onEventProcNanos, long deadlineProcNanos) {
			_clock = clock;
			_deadlinePeriod = deadlinePeriod;
			_onEventProcNanos = onEventProcNanos;
			_deadlineProcNanos = deadlineProcNanos;
		}
		
		@Override
		public void onEvent(byte[] event, long sequence, boolean isEndOfBatch)
				throws Exception {
			_onEventCallCount++;
			_val ^= busySpin(_onEventProcNanos);
		}
		
		@Override
		public void onDeadline() {
			_deadlineCallCount++;
			_val ^= busySpin(_deadlineProcNanos);
		}
		
		private long busySpin(long nanos) {
			long deadline = System.nanoTime() + nanos;
			while (System.nanoTime() < deadline);
			return deadline;
		}
		
		@Override
		public long moveToNextDeadline(long pendingCount) {
			_nextDeadline = _clock.currentMillis() + _deadlinePeriod;
			return _nextDeadline;
		}
		
		@Override
		public long getDeadline() {
			return _nextDeadline;
		}
		
		public long onDeadlineCallCount() {
			return _deadlineCallCount;
		}
		
		public long onEventCallCount() {
			return _onEventCallCount;
		}
		
		public long getTestVal() {
			return _val;
		}

		@Override
		public String name() {
			return "test";
		}
	}
	
}
