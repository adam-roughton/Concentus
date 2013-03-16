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
