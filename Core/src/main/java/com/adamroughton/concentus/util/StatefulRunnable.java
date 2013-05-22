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
package com.adamroughton.concentus.util;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StatefulRunnable<TRunnable extends Runnable> implements Runnable {

	private final Lock _lock = new ReentrantLock();
	private final Condition _condition = _lock.newCondition();
	private State _state = State.STOPPED;
	private final TRunnable _runnable;
	
	public StatefulRunnable(final TRunnable runnable) {
		_runnable = Objects.requireNonNull(runnable);
	}
	
	@Override
	public void run() {
		if (!changeState(State.RUNNING, State.STOPPED))
				throw new RuntimeException("Only one instance of the runnable can be running at any given time.");
		_runnable.run();
		changeState(State.STOPPED, State.RUNNING);
	}
	
	public enum State {
		STOPPED,
		RUNNING
	}
	
	public TRunnable getWrappedRunnable() {
		return _runnable;
	}
	
	public State getState() {
		_lock.lock();
		try {
			return _state;
		} finally {
			_lock.unlock();
		}
	}
	
	public void waitForState(State state) throws InterruptedException {
		_lock.lock();
		try {
			while (_state != state) {
				_condition.await();
			}
		} finally {
			_lock.unlock();
		}
	}
	
	public void waitForState(State state, long timeout, TimeUnit unit) throws InterruptedException {
		_lock.lock();
		try {
			while (_state != state) {
				_condition.await(timeout, unit);
			}
		} finally {
			_lock.unlock();
		}
	}
	
	private boolean changeState(State newState, State expectedState) {
		_lock.lock();
		try {
			if (_state != expectedState) return false;
			_state = newState;
			_condition.signalAll();
		} finally {
			_lock.unlock();
		}
		return true;
	}

}
