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
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class MessagingUtil {
	
	public static <TBuffer extends ResizingBuffer> EventProcessor asSocketOwner(
			String name,
			EventQueue<TBuffer> eventQueue, 
			MessengerDependentEventHandler<TBuffer> eventHandler, 
			final Mutex<Messenger<TBuffer>> messengerMutex,
			Sequence...gatingSequences) {	
		final EventHandlerAdapter<TBuffer> adapter = new EventHandlerAdapter<>(eventHandler);
		return eventQueue.createEventProcessor(name, new EventProcessorFactory<TBuffer, EventProcessor>() {

			@Override
			public EventProcessor createProcessor(
					final DataProvider<TBuffer> eventProvider, final SequenceBarrier barrier) {
				return new EventProcessor() {

					private final BatchEventProcessor<TBuffer> _batchProcessor = 
							new BatchEventProcessor<>(eventProvider, barrier, adapter);
					
					@Override
					public void run() {
						messengerMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

							@Override
							public void asOwner(Messenger<TBuffer> messenger) {
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
	
	public interface MessengerDependentEventHandler<TBuffer extends ResizingBuffer> {
		void onEvent(TBuffer event, long sequence, boolean endOfBatch, Messenger<TBuffer> messenger)
			throws Exception;
	}
	
	private static final class EventHandlerAdapter<TBuffer extends ResizingBuffer> implements EventHandler<TBuffer> {

		private final MessengerDependentEventHandler<TBuffer> _eventHandler;
		private Messenger<TBuffer> _messenger;
		
		public EventHandlerAdapter(MessengerDependentEventHandler<TBuffer> eventHandler) {
			_eventHandler = eventHandler;
		}
		
		public void setMessenger(Messenger<TBuffer> messenger) {
			_messenger = messenger;
		}
		
		@Override
		public void onEvent(TBuffer event, long sequence, boolean endOfBatch)
				throws Exception {
			_eventHandler.onEvent(event, sequence, endOfBatch, _messenger);
		}
		
	}
	
}
