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
package com.adamroughton.concentus.messaging;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class BlockingCall {
	
	private final Lock _lock = new ReentrantLock();
	private final Condition _blockCond = _lock.newCondition();
	private final Condition _callerHasBlockedCond = _lock.newCondition();
	
	private boolean _raiseEterm = false;
	private boolean _interrupt = false;
	private boolean _hasBlocked = false;
	
	public boolean waitForBlockingCall(long timeout, TimeUnit unit) throws InterruptedException {
		boolean success = false;
		_lock.lock();
		try {
			if (timeout >= 0) {
				long endTime = System.nanoTime() + unit.toNanos(timeout);
				long remainingNanos;
				while ((remainingNanos = endTime - System.nanoTime()) > 0 && !_hasBlocked) {
					success = _callerHasBlockedCond.await(remainingNanos, TimeUnit.NANOSECONDS);
				}
			} else {
				while(!_hasBlocked) {
					_callerHasBlockedCond.await();
					success = true;
				}
			}
		} finally {
			_lock.unlock();
		}
		return success;
	}
	
	public void waitForBlockingCall() throws InterruptedException {
		waitForBlockingCall(-1, TimeUnit.NANOSECONDS);
	}
	
	public void releaseBlockedCall() {
		_lock.lock();
		try {
			_blockCond.signal();
		} finally {
			_lock.unlock();
		}
	}
	
	public void interruptBlockedCall() {
		_lock.lock();
		try {
			if (_hasBlocked) {
				_interrupt = true;
			}
			_blockCond.signal();
		} finally {
			_lock.unlock();
		}
	}
	
	public void raiseEtermOnBlockedCall() {
		_lock.lock();
		try {
			// only raise if actually blocked
			if (_hasBlocked) {
				_raiseEterm = true;
			}
			_blockCond.signal();
		} finally {
			_lock.unlock();
		}
	}
	
	public void block() {
		boolean raiseEterm;
		boolean interrupt;
		
		_lock.lock();
		try {
			_hasBlocked = true;
			_callerHasBlockedCond.signalAll();
			try {
				_blockCond.await();
			} catch (InterruptedException eInterrupted) {
				Thread.currentThread().interrupt();
			}
			raiseEterm = _raiseEterm;
			interrupt = _interrupt;
		} finally {
			_lock.unlock();
		}
		if (raiseEterm) {
			throw new ZMQException("Fake Socket Close", (int) ZMQ.Error.ETERM.getCode());
		} else if (interrupt) {
			Thread.currentThread().interrupt();
		}
	}
	
}