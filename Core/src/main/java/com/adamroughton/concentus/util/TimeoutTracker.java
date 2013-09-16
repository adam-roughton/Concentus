package com.adamroughton.concentus.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class TimeoutTracker {

	private final long _startTime;
	private final long _deadline;
	
	public TimeoutTracker(long timeout, TimeUnit unit) {
		_startTime = System.currentTimeMillis();
		_deadline = _startTime + unit.toMillis(timeout);
	}
	
	public boolean hasTimedOut() {
		return remainingMillis() == 0;
	}
	
	public void assertTimeRemaining() throws TimeoutException {
		if (hasTimedOut()) throw new TimeoutException();
	}
	
	public long remainingMillis() {
		return Math.max(0, _deadline - System.currentTimeMillis());
	}
	
	public TimeUnit getUnit() {
		return TimeUnit.MILLISECONDS;
	}
	
	public long getTimeout() {
		return remainingMillis();
	}
	
}
