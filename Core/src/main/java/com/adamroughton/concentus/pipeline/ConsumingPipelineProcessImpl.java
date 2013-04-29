package com.adamroughton.concentus.pipeline;

import com.adamroughton.concentus.Clock;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;

public class ConsumingPipelineProcessImpl<TEvent> extends ProducingPipelineProcessImpl<TEvent> implements ConsumingPipelineProcess<TEvent> {
	
	private EventProcessor _consumer;
	
	public ConsumingPipelineProcessImpl(EventProcessor consumer, Clock clock) {
		super(consumer, clock);
		_consumer = consumer;
	}
	
	@Override
	public void halt() {
		super.halt();
		_consumer.halt();
	}

	@Override
	public Sequence getSequence() {
		return _consumer.getSequence();
	}
	
	@Override
	public String toString() {
		return String.format("Consuming Pipeline Process wrapping '%s'", _consumer.toString());
	}

}
