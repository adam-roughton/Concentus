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
package com.adamroughton.concentus.pipeline;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

class PipelineSegment<TEvent> {
	
	private final PipelineProcess<TEvent> _process;
	private final Collection<RingBuffer<TEvent>> _connectors;
	private final Clock _clock;
	private boolean _isStarted = false;
	
	public PipelineSegment(PipelineProcess<TEvent> process, Clock clock) {
		this(Collections.<RingBuffer<TEvent>>emptyList(), process, clock);
	}
	
	public PipelineSegment(
			Collection<RingBuffer<TEvent>> connectors, 
			ConsumingPipelineProcess<TEvent> process,
			Clock clock) {
		this(connectors, (PipelineProcess<TEvent>) process, clock);
	}
	
	private PipelineSegment(
			Collection<RingBuffer<TEvent>> connectors, 
			PipelineProcess<TEvent> process,
			Clock clock) {
		_connectors = Objects.requireNonNull(connectors);
		_process = Objects.requireNonNull(process);
		_clock = Objects.requireNonNull(clock);
	}
	
	public void start(Executor executor) {
		if (!_isStarted) {
			executor.execute(_process);
			_isStarted = true;
		}
	}
	
	public void halt(long timeout, TimeUnit unit) throws InterruptedException {
		if (!_isStarted) return;
		
		Log.info(String.format("Halting - pipeline process: %s...", _process.toString()));
		
		long startTime = _clock.nanoTime();
		long deadline = startTime + unit.toNanos(timeout);
		
		if (_process instanceof ConsumingPipelineProcess<?>) {
			ConsumingPipelineProcess<TEvent> consumingProcess = (ConsumingPipelineProcess<TEvent>) _process;
			/*
			 * Halt the consumers, ensuring all pending events are consumed
			 */
			for (RingBuffer<TEvent> connector : _connectors) {
				long finalCursor = connector.getCursor();
				Sequence consumerSeq = consumingProcess.getSequence();
				
				if (!consumingProcess.isRunning() && consumerSeq.get() != finalCursor) {
					throw new RuntimeException(String.format("The consumer (%s) was found to be halted, " +
							"but there are still events to be consumed.", consumingProcess.toString()));
				}
				while (consumingProcess.getSequence().get() != finalCursor) {
					if (Util.nanosUntil(deadline, _clock) <= 0) {
						throw new InterruptedException(String.format("Timed out waiting for the consumer (%s) " +
								"to finish consuming remaining events.", consumingProcess.toString()));
					}
				}
			}
		}
		_process.halt();
		_process.awaitHalt(Util.nanosUntil(deadline, _clock), TimeUnit.NANOSECONDS);
		
		_isStarted = false;
		Log.info(String.format("Halted - pipeline process: %s", _process.toString()));
	}
	
}
