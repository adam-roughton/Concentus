package com.adamroughton.concentus;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;

public interface ComponentResolver<TBuffer extends ResizingBuffer> {

	ZmqSocketManager<TBuffer> newSocketManager(Clock clock);
	
	EventQueueFactory getEventQueueFactory();
	
}
