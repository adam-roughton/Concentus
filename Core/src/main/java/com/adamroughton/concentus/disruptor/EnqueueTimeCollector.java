package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.util.PaddedLong;

final class EnqueueTimeCollector {

	private final PaddedLong[] _enqueueTimes;
	private final int _mask;
	
	public EnqueueTimeCollector(int queueSize) {
		if (Integer.bitCount(queueSize) != 1)
			throw new IllegalArgumentException(String.format("The queue size must be a power of 2 (was %d)", queueSize));
		_enqueueTimes = new PaddedLong[queueSize];
		for (int i = 0; i < queueSize; i++) {
			_enqueueTimes[i] = new PaddedLong(-1);
		}
		_mask = queueSize - 1;
	}
	
	public void setEnqueueTime(long sequence, long enqueueTime) {
		_enqueueTimes[(int)(sequence & _mask)].set(enqueueTime);
	}
	
	public long getEnqueueTime(long sequence) {
		return _enqueueTimes[(int)(sequence & _mask)].get();
	}
	
}
