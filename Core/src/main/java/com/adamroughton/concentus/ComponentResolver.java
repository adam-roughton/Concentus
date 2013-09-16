package com.adamroughton.concentus;

import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.SocketManager;

public interface ComponentResolver<TBuffer extends ResizingBuffer> {

	SocketManager<TBuffer> newSocketManager(Clock clock);
	
	EventQueueFactory getEventQueueFactory();
	
}
