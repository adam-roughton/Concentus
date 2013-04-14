package com.adamroughton.concentus;

import java.util.concurrent.TimeUnit;

public class DrivableClock implements Clock {

	private long _nanoTime = 0;
	
	@Override
	public long currentMillis() {
		return TimeUnit.NANOSECONDS.toMillis(_nanoTime);
	}

	@Override
	public long nanoTime() {
		return _nanoTime;
	}
	
	public void setTime(long time, TimeUnit unit) {
		_nanoTime = unit.toNanos(time);
	}

	public void advance(long timeDiff, TimeUnit unit) {
		_nanoTime += unit.toNanos(timeDiff);
	}
	
}
