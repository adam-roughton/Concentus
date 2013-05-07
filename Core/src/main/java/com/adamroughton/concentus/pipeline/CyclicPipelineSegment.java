package com.adamroughton.concentus.pipeline;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

/**
 * Specialised segment that is aware of the cyclic dependency that flows from 
 * the connector to the process back to the connector again (perhaps indirectly).
 * On the first call to {@link CyclicPipelineSegment#halt(long, TimeUnit)}, the process
 * of this segment is stopped, to prevent events being created for the rest of the pipeline, and is
 * replaced with a no-op consumer: this keeps the rest of the pipeline flowing while each
 * segment is halted in turn. Finally when this segment is halted again (as the final part of
 * the pipeline), the no-op consumer is halted.
 * @author Adam Roughton
 *
 * @param <TEvent>
 */
class CyclicPipelineSegment<TEvent> extends PipelineSegment<TEvent> {

	private enum State {
		INIT,
		STARTED,
		CONSUME_ONLY,
		HALTED
	}
	
	private final RingBuffer<TEvent> _cyclicConnector;
	private final ConsumingPipelineProcess<TEvent> _process;
	private final Clock _clock;
	private State _state;
	private Executor _executor;
	private NoOpConsumer _consumer;
	
	public CyclicPipelineSegment(RingBuffer<TEvent> cyclicConnector,
			ConsumingPipelineProcess<TEvent> process, Clock clock) {
		this(cyclicConnector, Collections.<RingBuffer<TEvent>>emptyList(), process, clock);
	}

	public CyclicPipelineSegment(RingBuffer<TEvent> cyclicConnector, Collection<RingBuffer<TEvent>> connectors,
			ConsumingPipelineProcess<TEvent> process, Clock clock) {
		super(connectors, process, clock);
		_cyclicConnector = Objects.requireNonNull(cyclicConnector);
		_process = Objects.requireNonNull(process);
		_clock = Objects.requireNonNull(clock);
		_state = State.INIT;
	}

	@Override
	public void start(Executor executor) {
		super.start(executor);
		_executor = executor;
		_state = State.STARTED;
	}

	@Override
	public void halt(long timeout, TimeUnit unit) throws InterruptedException {
		long startTime = _clock.nanoTime();
		long deadline = startTime + unit.toNanos(timeout);
		
		if (_state == State.STARTED) {
			super.halt(timeout, unit);
			SequenceBarrier consumerBarrier = _cyclicConnector.newBarrier();
			_consumer = new NoOpConsumer(_process.getSequence(), consumerBarrier);
			_executor.execute(_consumer);
			_state = State.CONSUME_ONLY;
		} else if (_state == State.CONSUME_ONLY) {
			long finalCursor = _cyclicConnector.getCursor();
			while (_consumer.getSequence().get() != finalCursor) {
				if (Util.nanosUntil(deadline, _clock) <= 0) {
					throw new InterruptedException(String.format("Timed out waiting for the no-op consumer (%s) " +
							"to finish consuming remaining events.", _consumer.toString()));
				}
			}
			_consumer.halt();
			_state = State.HALTED;
		}
	}
	
	private static class NoOpConsumer implements EventProcessor {

		private final Sequence _sequence;
		private final SequenceBarrier _barrier;
		private final AtomicBoolean _running = new AtomicBoolean();
		
		public NoOpConsumer(Sequence sequence, SequenceBarrier barrier) {
			_sequence = sequence;
			_barrier = barrier;
		}
		
		@Override
		public void run() {
			if (!_running.compareAndSet(false, true))
				throw new IllegalStateException("The NoOpConsumer can only be started once.");
			_barrier.clearAlert();

			try {
				while (true) {
					_sequence.set(_barrier.waitFor(_sequence.get() + 1));
				}
			} catch (AlertException | InterruptedException eEscape) {
				// Do nothing
			}
		}

		@Override
		public Sequence getSequence() {
			return _sequence;
		}

		@Override
		public void halt() {
			_barrier.alert();
		}
		
	}
	
	

}
