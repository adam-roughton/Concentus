package com.adamroughton.concentus.canonicalstate;

import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.lmax.disruptor.RingBuffer;

/**
 * Service that handles collecting actions from 
 * @author Adam Roughton
 *
 */
public class ActionCollectorService<TBuffer extends ResizingBuffer> {

	private final String _collectorAddress;
	private RingBuffer<TBuffer> _recvBuffer;
	private RingBuffer<TBuffer> _sendBuffer;
	
	public ActionCollectorService(String collectorAddress) {
		_collectorAddress = collectorAddress;
	}
	
	public void start() {
		
	}
	
	public void stop() {
		
	}
	
	

}
