package com.adamroughton.concentus.disruptor;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
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
		
		long nextSequence = _sequence.get() + 1;
		while(true) {
			try {
				//_barrier.checkAlert();
				
				long pendingCount = _ringBuffer.getCursor() - (nextSequence - 1);
				long nextDeadline = _eventHandler.moveToNextDeadline(pendingCount);
				
				// wait until the next sequence
				long remainingTime = 0;
				long availableSequence = -1;
				do {
					if (nextSequence <= availableSequence) {
						_eventHandler.onEvent(_ringBuffer.get(nextSequence), nextSequence, nextDeadline);
						 nextSequence++;
					} else {
						_sequence.set(nextSequence - 1);
						remainingTime = timeUntil(nextDeadline);
						availableSequence = _barrier.waitFor(nextSequence, 
								remainingTime, TimeUnit.MILLISECONDS);
					}
				} while (remainingTime > 0);
				
				
//				while ((remainingTime = timeUntil(nextDeadline)) > 0) {
//					if (nextSequence <= availableSequence) {
//						_eventHandler.onEvent(_ringBuffer.get(nextSequence), nextSequence, nextDeadline);
//						 nextSequence++;
//					} else {
//						availableSequence = _barrier.waitFor(nextSequence, 
//								remainingTime, TimeUnit.MILLISECONDS);
//					}
//					
//				}
				_eventHandler.onDeadline();
				
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
