package com.adamroughton.concentus.disruptor;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
	
	private final DeadlineBasedEventHandler<T> _eventHandler;
	private final RingBuffer<T> _ringBuffer;
	private final SequenceBarrier _barrier;
	private final FatalExceptionCallback _exceptionCallback;
	
	public DeadlineBasedEventProcessor(DeadlineBasedEventHandler<T> eventHandler,
			RingBuffer<T> ringBuffer,
			SequenceBarrier barrier,
			FatalExceptionCallback exceptionCallback) {
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
		long halfBufferSize = _ringBuffer.getBufferSize() / 2;
		long lastForcedEventCount = 0;
		while(true) {
			try {
				long nextDeadline = _eventHandler.moveToNextDeadline(lastForcedEventCount);
				
				// we need to ensure that events get processed eventually
				long forceEventCount;
				long pendingCount = _ringBuffer.getCursor() - nextSequence;
				if (pendingCount > halfBufferSize) {
					forceEventCount = (pendingCount - halfBufferSize) * 2;
				} else {
					forceEventCount = 0;
				}
				
				// wait until the next sequence
				long remainingTime = 0;
				long availableSequence = -1;
				while (forceEventCount > 0 || (remainingTime = timeUntil(nextDeadline)) > 0) {
					if (nextSequence <= availableSequence) {
						_eventHandler.onEvent(_ringBuffer.get(nextSequence), nextSequence, nextDeadline);
						 nextSequence++;
						 forceEventCount--;
					} else {
						availableSequence = _barrier.waitFor(nextSequence, 
								remainingTime, TimeUnit.MILLISECONDS);
					}
					
				}
				_eventHandler.onDeadline();

				_sequence.set(nextSequence - 1);
				
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

	private static long timeUntil(long deadline) {
		long remainingTime = deadline - System.currentTimeMillis();
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
