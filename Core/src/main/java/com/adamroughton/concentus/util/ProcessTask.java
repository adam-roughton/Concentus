package com.adamroughton.concentus.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adamroughton.concentus.util.ProcessTask.ProcessDelegate.ReasonType;

public final class ProcessTask implements Runnable {

	public static interface ProcessDelegate {
		
		public enum ReasonType {
			STOPPED,
			ERROR
		}
		
		void configureProcess(Process process) throws InterruptedException;
		void onStop(ReasonType reason, int retCode, String stdoutBuffer, String stdErrBuffer, Exception e);
	}
	
	public enum State {
		INIT,
		RUNNING,
		STOPPED
	}
	
	private final Logger _log = LoggerFactory.getLogger(ProcessTask.class);
	
	private final AtomicReference<ProcessTask.State> _state = new AtomicReference<>(State.INIT);
	
	private final String _name;
	private final int _stdOutBufferLength;
	private final int _stdErrBufferLength;
	private final ProcessBuilder _processBuilder;
	private final ProcessTask.ProcessDelegate _delegate;
	private final Lock _lock = new ReentrantLock();
	private final Condition _processStateCondition = _lock.newCondition();

	private volatile Thread _processThread = null;
	
	public ProcessTask(String name, 
			int stdOutBufferLength,
			int stdErrBufferLength,
			List<String> commands, 
			ProcessTask.ProcessDelegate delegate) {
		_name = Objects.requireNonNull(name);
		_stdOutBufferLength = stdOutBufferLength;
		_stdErrBufferLength = stdErrBufferLength;
		_processBuilder = new ProcessBuilder(commands);
		_delegate = Objects.requireNonNull(delegate);
	}
	
	@Override
	public void run() {
		if (!_state.compareAndSet(State.INIT, State.RUNNING)) {
			_log.warn("Tried to start process task more than once!");
			return;
		}
		_processThread = Thread.currentThread();
		
		final Process process;
		ProcessPipeConsumer stdOutConsumer = null;
		ProcessPipeConsumer stdErrConsumer = null;
		
		Thread stdOutConsumerThread = null;
		Thread stdErrConsumerThread = null;
		
		
		try {
			process = _processBuilder.start();
		} catch (IOException eStart) {
			_delegate.onStop(ReasonType.ERROR, -1, null, null, eStart);
			_state.set(State.STOPPED);
			return;
		}
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				
				public void run() {
					destroyIfRunning(process);
				}
				
			});
			_delegate.configureProcess(process);
			
			stdOutConsumer = new ProcessPipeConsumer(_name, _stdOutBufferLength, process.getInputStream());
			stdErrConsumer = new ProcessPipeConsumer(_name, _stdErrBufferLength, process.getErrorStream());
			
			stdOutConsumerThread = new Thread(stdOutConsumer);
			stdErrConsumerThread = new Thread(stdErrConsumer);
			
			stdOutConsumerThread.start();
			stdErrConsumerThread.start();
			
			process.waitFor();
		} catch (InterruptedException eInterrupted) {
			destroyIfRunning(process);
		} finally {
			while (_state.get() == State.RUNNING) {
				try {
					int retCode = process.waitFor();
					_log.info(_name + ":> " + "Got return code " + retCode);
					
					String stdOutBuffer = "";
					String stdErrBuffer = "";
					if (stdOutConsumerThread != null) {
						stdOutConsumerThread.join();
						stdOutBuffer = stdOutConsumer.getPipeBuffer().toString();
					}
					if (stdErrConsumerThread != null) {
						stdErrConsumerThread.join();
						stdErrBuffer = stdErrConsumer.getPipeBuffer().toString();
					}
					
					_delegate.onStop(ReasonType.STOPPED, retCode, stdOutBuffer,	stdErrBuffer, null);
					_state.set(State.STOPPED);
				} catch (InterruptedException e) {
					_log.warn("Interrupted waiting for process to terminate - looping until process is dead.");
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
		return _state.get() == State.STOPPED;
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
	
	private static class ProcessPipeConsumer implements Runnable {
		private final Logger _log = LoggerFactory.getLogger(ProcessTask.class);
		
		private final AtomicBoolean _isRunning = new AtomicBoolean(false);
		private final CircularStringBuffer _pipeBuffer;
		private final String _prefix;
		private final InputStream _pipe;
		
		public ProcessPipeConsumer(String prefix, int bufferLength, InputStream pipe) {
			_prefix = prefix;
			_pipeBuffer = new CircularStringBuffer(bufferLength);
			_pipe = Objects.requireNonNull(pipe);
		}
		
		@Override
		public void run() {
			if (!_isRunning.compareAndSet(false, true)) {
				throw new IllegalStateException("This process pipe to log adapter is already started");
			}
			try (BufferedReader pipeReader = new BufferedReader(new InputStreamReader(_pipe))) {
				String readLine;
				do {
					readLine = pipeReader.readLine();
					if (readLine != null) {
						_pipeBuffer.append(readLine + '\n');
						_log.info(_prefix + ":> " + readLine);
					}
				} while (readLine != null);
			} catch (IOException eIO) {
			} finally {
				_isRunning.set(false);
			}
		}
		
		public CircularStringBuffer getPipeBuffer() {
			return _pipeBuffer;
		}
	}
}