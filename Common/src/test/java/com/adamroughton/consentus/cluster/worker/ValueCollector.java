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
package com.adamroughton.consentus.cluster.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ValueCollector<TValue> {

	private final List<TValue> _collectedValues = new ArrayList<>();
	private final Lock _lock = new ReentrantLock();
	private final Condition _condition = _lock.newCondition();
	
	public void addValue(TValue value) {
		_lock.lock();
		try {
			_collectedValues.add(value);
			_condition.signalAll();
		} finally {
			_lock.unlock();
		}
	}
	
	public List<TValue> getValues() {
		_lock.lock();
		try {
			return new ArrayList<>(_collectedValues);
		} finally {
			_lock.unlock();
		}
	}
	
	public void waitForCount(int count) throws InterruptedException {
		_lock.lock();
		try {
			while (_collectedValues.size() < count) {
				_condition.wait();
			}
		} finally {
			_lock.unlock();
		}
	}
	
	public void waitForCount(int count, long timeout, TimeUnit unit) throws InterruptedException {
		long remainingTime = unit.toNanos(timeout);
		long startTime = System.nanoTime();
		
		_lock.lock();
		try {
			while (_collectedValues.size() < count) {
				long elapsedTime = System.nanoTime() - startTime;
				remainingTime -= elapsedTime;
				if (remainingTime > 0) {
					_condition.await(remainingTime, TimeUnit.NANOSECONDS);
				} else return;
			}
		} finally {
			_lock.unlock();
		}
	}
	
}
