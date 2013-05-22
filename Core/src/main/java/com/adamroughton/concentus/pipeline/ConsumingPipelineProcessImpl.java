/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
