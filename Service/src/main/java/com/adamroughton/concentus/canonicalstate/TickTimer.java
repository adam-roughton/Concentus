package com.adamroughton.concentus.canonicalstate;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.util.Util;

public final class TickTimer implements Runnable {
	
	public interface TickStrategy {
		void onTick(long time);
	}
	
	private final AtomicBoolean _isRunning = new AtomicBoolean(false);
	private volatile Thread _timerThread;
	private boolean _allowTick;
	
	private final Clock _clock;
	private final TickStrategy _tickStrategy;
	private final long _tickDuration;
	private final long _simStartTime;
	private final FatalExceptionCallback _exCallback;
	
	private final Lock _lock = new ReentrantLock();
	private final Condition _condition = _lock.newCondition();
	
	public TickTimer(Clock clock, TickStrategy tickStrategy, long tickDuration, long simStartTime, 
			FatalExceptionCallback exCallback) {
		_clock = Objects.requireNonNull(clock);
		_tickStrategy = Objects.requireNonNull(tickStrategy);
		_tickDuration = tickDuration;
		_simStartTime = simStartTime;
		_exCallback = Objects.requireNonNull(exCallback);
	}
	
	@Override
	public void run() {
		try {
			if (!_isRunning.compareAndSet(false, true)) {
				throw new IllegalStateException("This tick timer instance can only be active on one thread at a time.");
			}
			_timerThread = Thread.currentThread();
			
			final long startWallTime = _clock.currentMillis();
			long nextTickSimTime = _simStartTime;
			try {
				while (!Thread.interrupted()) {
					_lock.lock();
					try {
						while (!_allowTick) {
							_condition.await();
						}
						_allowTick = false;
					} finally {
						_lock.unlock();
					}
					
					// wait until the tick is due
					long timeUntilTick = Util.millisUntil((nextTickSimTime - _simStartTime) + startWallTime, _clock);
					Thread.sleep(timeUntilTick);
					
					// do the tick
					_tickStrategy.onTick(nextTickSimTime);
					long simTime = (_clock.currentMillis() - startWallTime) + _simStartTime;
					
					// set the next tick to the next available tick time (skip over any missed ticks)
					nextTickSimTime = ((simTime / _tickDuration) + 1) * _tickDuration;
				}
			} catch (InterruptedException eInterrupted) {
			}
		} catch (Exception e) {
			_exCallback.signalFatalException(e);
		} finally {
			_isRunning.set(false);
		}
	}
	
	public void allowNextTick() {
		_lock.lock();
		try {
			_allowTick = true;
			_condition.signalAll();
		} finally {
			_lock.unlock();
		}		
	}
	
	public void stop() {
		Thread timerThread = _timerThread;
		if (timerThread != null) {
			timerThread.interrupt();
		}
	}
	
}