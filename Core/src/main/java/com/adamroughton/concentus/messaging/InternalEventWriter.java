package com.adamroughton.concentus.messaging;

import java.util.Objects;

import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;

public final class InternalEventWriter<TBuffer extends ResizingBuffer> {
	
	private final IncomingEventHeader _header;
	private final Kryo _kryo;
	private final EventQueuePublisher<TBuffer> _internalPublisher;
	
	public InternalEventWriter(
			String name,
			EventQueue<TBuffer> eventQueue, 
			IncomingEventHeader header, 
			KryoRegistratorDelegate kryoRegistratorDelegate,
			boolean isBlocking) {
		_internalPublisher = eventQueue.createPublisher(name, isBlocking);
		_header = Objects.requireNonNull(header);
		_kryo = Util.newKryoInstance();
		if (kryoRegistratorDelegate != null) {
			kryoRegistratorDelegate.register(_kryo);
		}
	}
	
	public boolean writeEvent(Object event) {
		TBuffer buffer = _internalPublisher.next();
		
		int contentOffset = _header.getEventOffset();
		ResizingBuffer contentSlice = buffer.slice(contentOffset);
		Util.toKryoBytes(_kryo, event, contentSlice);
		
		_header.setSegmentMetaData(buffer, 0, contentOffset, contentSlice.getContentSize());
		_header.setFromInternal(buffer);
		_header.setIsValid(buffer, true);
		_header.setIsMessagingEvent(buffer, false);
		
		return _internalPublisher.publish();
	}
	
}
