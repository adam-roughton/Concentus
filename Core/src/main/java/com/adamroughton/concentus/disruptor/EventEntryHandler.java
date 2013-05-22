package com.adamroughton.concentus.disruptor;

import com.lmax.disruptor.EventFactory;

public interface EventEntryHandler<TEvent> extends EventFactory<TEvent> {

	void clear(TEvent event);
	
	void copy(TEvent source, TEvent destination);
	
}
