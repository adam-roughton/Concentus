package com.adamroughton.concentus.pipeline;

import java.util.concurrent.TimeUnit;

public interface PipelineProcess<TEvent> extends Runnable {

	void halt();
	
	void awaitHalt() throws InterruptedException;
	
	void awaitHalt(long timeout, TimeUnit unit) throws InterruptedException;
	
	boolean isRunning();
}
