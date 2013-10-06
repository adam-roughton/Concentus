package com.adamroughton.concentus.util;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.esotericsoftware.minlog.Log;

public final class ProcessTask implements Runnable {

	public static interface ProcessDelegate {
		void runProcess(Process process) throws InterruptedException;
		void handleError(Exception e);
	}
	
	public enum State {
		INIT,
		RUNNING,
		STOPPED
	}
	
	private final AtomicReference<ProcessTask.State> _state = new AtomicReference<>(State.INIT);
	
	private final ProcessBuilder _processBuilder;
	private final ProcessTask.ProcessDelegate _delegate;
	private final Lock _lock = new ReentrantLock();
	private final Condition _processStateCondition = _lock.newCondition();
	
	private volatile Thread _processThread = null;
	
	public ProcessTask(ProcessBuilder processBuilder, ProcessTask.ProcessDelegate delegate) {
		_processBuilder = Objects.requireNonNull(processBuilder);
		_delegate = Objects.requireNonNull(delegate);
	}
	
	@Override
	public void run() {
		if (!_state.compareAndSet(State.INIT, State.RUNNING)) {
			Log.warn("Tried to start process task more than once!");
			return;
		}
		_processThread = Thread.currentThread();
		
		final Process process;
		try {
			process = _processBuilder.start();
		} catch (IOException eStart) {
			_delegate.handleError(eStart);
			_state.set(State.STOPPED);
			return;
		}
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				
				public void run() {
					destroyIfRunning(process);
				}
				
			});
			_delegate.runProcess(process);
		} catch (InterruptedException eInterrupted) {
			destroyIfRunning(process);
		} finally {
			while (_state.get() == State.RUNNING) {
				try {
					int retVal = process.waitFor();
					Log.info("ProcessTask.run: got retval " + retVal);
					_state.set(State.STOPPED);
				} catch (InterruptedException e) {
				}
			}
			_lock.lock();
			try {
				_processStateCondition.signalAll();
			} finally {
				_lock.unlock();
			}
		}
	}
	
	public boolean hasStopped() {
		return _state.get() != State.STOPPED;
	}
	
	public void stop() {
		Thread processThread = _processThread;
		if (processThread != null) {
			processThread.interrupt();
		}
	}
	
	public void waitForStop(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
		_lock.lock();
		try {
			if (timeout == -1) {
				while (!hasStopped()) {
					_processStateCondition.await();
				}
			} else {
				TimeoutTracker timeoutTracker = new TimeoutTracker(timeout, unit);
				while (!hasStopped() && !timeoutTracker.hasTimedOut()) {
					_processStateCondition.await(timeoutTracker.getTimeout(), timeoutTracker.getUnit());
				}
				if (!hasStopped()) {
					throw new TimeoutException("Timed out waiting for process to stop.");
				}
			}
		} finally {
			_lock.unlock();
		}
	}
	
	public void waitForStop() throws InterruptedException {
		try {
			waitForStop(-1, TimeUnit.NANOSECONDS);
		} catch (TimeoutException e) {
			// not on the -1 path
		}
	}
	
	private static void destroyIfRunning(Process process) {
		try {
			process.exitValue();
		} catch (IllegalThreadStateException eNotStopped) {
			process.destroy();
		}
	}
}