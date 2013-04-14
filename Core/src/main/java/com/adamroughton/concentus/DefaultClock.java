package com.adamroughton.concentus;

public class DefaultClock implements Clock {

	@Override
	public long currentMillis() {
		return System.currentTimeMillis();
	}

	@Override
	public long nanoTime() {
		return System.nanoTime();
	}

}
