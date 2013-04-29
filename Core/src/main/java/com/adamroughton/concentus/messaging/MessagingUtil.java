package com.adamroughton.concentus.messaging;

import com.adamroughton.concentus.messaging.SocketMutex.SocketSetMutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

public class MessagingUtil {

	public static <TEvent> EventProcessor asSocketOwner(
			final RingBuffer<TEvent> ringBuffer, 
			final SequenceBarrier sequenceBarrier,
			final SocketDependentEventHandler<TEvent, SocketPackage> eventHandler, 
			final SocketMutex socketMutex) {	
		final EventHandlerAdapter<TEvent, SocketPackage> adapter = new EventHandlerAdapter<>(eventHandler);
		
		return new EventProcessor() {

			private final BatchEventProcessor<TEvent> _batchProcessor = 
					new BatchEventProcessor<>(ringBuffer, sequenceBarrier, adapter);
			
			@Override
			public void run() {
				socketMutex.runAsOwner(new OwnerDelegate<SocketPackage>() {

					@Override
					public void asOwner(SocketPackage socketPackage) {
						adapter.setSocketPackage(socketPackage);
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
	
	public static <TEvent, TSocketSet extends SocketSet> EventProcessor asSocketOwner(
			final RingBuffer<TEvent> ringBuffer, 
			final SequenceBarrier sequenceBarrier,
			final SocketDependentEventHandler<TEvent, TSocketSet> eventHandler, 
			final SocketSetMutex<TSocketSet> socketSetMutex) {	
		final EventHandlerAdapter<TEvent, TSocketSet> adapter = new EventHandlerAdapter<>(eventHandler);
		
		return new EventProcessor() {

			private final BatchEventProcessor<TEvent> _batchProcessor = 
					new BatchEventProcessor<>(ringBuffer, sequenceBarrier, adapter);
			
			@Override
			public void run() {
				socketSetMutex.runAsOwner(new OwnerDelegate<TSocketSet>() {

					@Override
					public void asOwner(TSocketSet socketSet) {
						adapter.setSocketPackage(socketSet);
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
	
	public interface SocketDependentEventHandler<TEvent, TSocketPackage> {
		void onEvent(TEvent event, long sequence, boolean endOfBatch, TSocketPackage socketPackage)
			throws Exception;
	}
	
	private static final class EventHandlerAdapter<TEvent, TSocketPackage> implements EventHandler<TEvent> {

		private final SocketDependentEventHandler<TEvent, TSocketPackage> _eventHandler;
		private TSocketPackage _socketPackage;
		
		public EventHandlerAdapter(SocketDependentEventHandler<TEvent, TSocketPackage> eventHandler) {
			_eventHandler = eventHandler;
		}
		
		public void setSocketPackage(TSocketPackage socketPackage) {
			_socketPackage = socketPackage;
		}
		
		@Override
		public void onEvent(TEvent event, long sequence, boolean endOfBatch)
				throws Exception {
			_eventHandler.onEvent(event, sequence, endOfBatch, _socketPackage);
		}
		
	}
	
}
