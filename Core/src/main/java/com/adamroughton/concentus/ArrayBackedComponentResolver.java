package com.adamroughton.concentus;

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketManagerImpl;

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
	public SocketManager<ArrayBackedResizingBuffer> newSocketManager(Clock clock) {
		return new SocketManagerImpl(clock);
	}
	
}
