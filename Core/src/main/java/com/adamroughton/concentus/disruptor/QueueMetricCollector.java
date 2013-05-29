package com.adamroughton.concentus.disruptor;

public interface QueueMetricCollector {

	void onEnqueue(long sequence);
	
}
