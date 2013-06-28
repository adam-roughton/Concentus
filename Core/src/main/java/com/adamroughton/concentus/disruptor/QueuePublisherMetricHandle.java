package com.adamroughton.concentus.disruptor;

interface QueuePublisherMetricHandle {

	/**
	 * Called just before the EventQueuePublisher
	 * releases the event queue slot for consumption.
	 * This is used for injecting metric collection into
	 * the event queue objects.
	 * @param sequence the sequence that is about to be published
	 */
	void onEnqueue(long sequence);
	
}
