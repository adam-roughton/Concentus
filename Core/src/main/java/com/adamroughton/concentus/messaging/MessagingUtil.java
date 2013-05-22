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
package com.adamroughton.concentus.messaging;

import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueue.EventProcessorFactory;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class MessagingUtil {
	
	public static <TEvent> EventProcessor asSocketOwner(
			EventQueue<TEvent> eventQueue, 
			MessengerDependentEventHandler<TEvent> eventHandler, 
			final Mutex<Messenger> messengerMutex,
			Sequence...gatingSequences) {	
		final EventHandlerAdapter<TEvent> adapter = new EventHandlerAdapter<>(eventHandler);
		return eventQueue.createEventProcessor(new EventProcessorFactory<TEvent, EventProcessor>() {

			@Override
			public EventProcessor createProcessor(
					final RingBuffer<TEvent> ringBuffer, final SequenceBarrier barrier) {
				return new EventProcessor() {

					private final BatchEventProcessor<TEvent> _batchProcessor = 
							new BatchEventProcessor<>(ringBuffer, barrier, adapter);
					
					@Override
					public void run() {
						messengerMutex.runAsOwner(new OwnerDelegate<Messenger>() {

							@Override
							public void asOwner(Messenger messenger) {
								adapter.setMessenger(messenger);
								_batchProcessor.run();
							}
							
						});
					}

					@Override
					public Sequence getSequence() {
						return _batchProcessor.getSequence();
					}

					@Override
					public void halt() {
						_batchProcessor.halt();
					}
					
				};
			}
		}, gatingSequences);
	}
	
	public interface MessengerDependentEventHandler<TEvent> {
		void onEvent(TEvent event, long sequence, boolean endOfBatch, Messenger messenger)
			throws Exception;
	}
	
	private static final class EventHandlerAdapter<TEvent> implements EventHandler<TEvent> {

		private final MessengerDependentEventHandler<TEvent> _eventHandler;
		private Messenger _messenger;
		
		public EventHandlerAdapter(MessengerDependentEventHandler<TEvent> eventHandler) {
			_eventHandler = eventHandler;
		}
		
		public void setMessenger(Messenger messenger) {
			_messenger = messenger;
		}
		
		@Override
		public void onEvent(TEvent event, long sequence, boolean endOfBatch)
				throws Exception {
			_eventHandler.onEvent(event, sequence, endOfBatch, _messenger);
		}
		
	}
	
}
