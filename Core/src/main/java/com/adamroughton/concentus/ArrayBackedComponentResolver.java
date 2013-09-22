package com.adamroughton.concentus;

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.ArrayBackedZmqSocketManagerImpl;

public class ArrayBackedComponentResolver implements ComponentResolver<ArrayBackedResizingBuffer> {
	
	private EventQueueFactory _eventQueueFactory;
	
	public ArrayBackedComponentResolver() {
		_eventQueueFactory = new StandardEventQueueFactory();
	}

	@Override
	public EventQueueFactory getEventQueueFactory() {
		return _eventQueueFactory;
	}

	@Override
	public ZmqSocketManager<ArrayBackedResizingBuffer> newSocketManager(Clock clock) {
		return new ArrayBackedZmqSocketManagerImpl(clock);
	}
	
}
