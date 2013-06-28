package com.adamroughton.concentus.disruptor;

public interface PrePublishDelegate {
	
	void beforePublish(long sequence);
	
}
