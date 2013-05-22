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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.minlog.Log;

public class ProducingPipelineProcessImpl<TEvent> implements PipelineProcess<TEvent> {

	private final Runnable _process;
	private final Lock _lock = new ReentrantLock();
	private final Condition _condition = _lock.newCondition();
	private final Clock _clock;
	
	private Thread _activeThread = null;
	
	public ProducingPipelineProcessImpl(Runnable process, Clock clock) {
		_process = Objects.requireNonNull(process);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public void run() {
		Log.info(String.format("Running process %s", _process.toString()));
		_lock.lock();
		try {
			if (_activeThread != null) 
				throw new RuntimeException(String.format("This instance (%s) can only " +
						"be active on one thread at a time.", _process.toString()));
			_activeThread = Thread.currentThread();
		} finally {
			_lock.unlock();
		}
		try {
			_process.run();
		} finally {
			_lock.lock();
			try {
				_activeThread = null;
				_condition.signalAll();
			} finally {
				_lock.unlock();
			}
		}
		Log.info(String.format("Leaving process %s", _process.toString()));
	}

	@Override
	public void halt() {
		_lock.lock();
		try {
			if (_activeThread != null) {
				_activeThread.interrupt();
			}
		} finally {
			_lock.unlock();
		}
	}

	@Override
	public void awaitHalt() throws InterruptedException {
		_lock.lock();
		try {
			while (_activeThread != null) {
				_condition.await();
			}
		} finally {
			_lock.unlock();
		}
	}

	@Override
	public void awaitHalt(long timeout, TimeUnit unit)
			throws InterruptedException {
		long startTime = _clock.nanoTime();
		long deadline = startTime + unit.toNanos(timeout);
		_lock.lock();
		try {
			while (_activeThread != null) {
				_condition.await(Util.nanosUntil(deadline, _clock), TimeUnit.NANOSECONDS);
			}
		} finally {
			_lock.unlock();
		}
	}

	@Override
	public boolean isRunning() {
		_lock.lock();
		try {
			return _activeThread != null;
		} finally {
			_lock.unlock();
		}
	}

	@Override
	public String toString() {
		return String.format("Producing Pipeline Process wrapping '%s'", _process.toString());
	}

}
