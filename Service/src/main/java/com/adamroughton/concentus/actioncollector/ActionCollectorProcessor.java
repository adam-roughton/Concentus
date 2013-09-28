package com.adamroughton.concentus.actioncollector;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.Iterator;
import java.util.Objects;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ActionReceiptEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientHandlerInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.TickEvent;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.RouterPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;

public final class ActionCollectorProcessor<TBuffer extends ResizingBuffer> implements EventHandler<TBuffer> {

	private final int _actionCollectorId;
	private final ActionProcessingLogic _actionProcessingLogic;
	private final TickDelegate _tickDelegate;
	private final SendQueue<OutgoingEventHeader, TBuffer> _sendQueue;
	private final IncomingEventHeader _recvHeader;
	
	private final ActionEvent _actionEvent = new ActionEvent();
	private final TickEvent _tickEvent = new TickEvent();
	private final ClientHandlerInputEvent _clientHandlerInputEvent = new ClientHandlerInputEvent();
	private final ActionReceiptEvent _actionReceiptEvent = new ActionReceiptEvent();
	
	private final Int2ObjectMap<StructuredSlidingWindowMap<ResizingBuffer>> _clientHandlerReliableEventStoreLookup = 
			new Int2ObjectOpenHashMap<>();
	
	public ActionCollectorProcessor(
			int actionCollectorId,
			IncomingEventHeader recvHeader,
			CollectiveApplication application, 
			TickDelegate tickDelegate, 
			SendQueue<OutgoingEventHeader, TBuffer> sendQueue,
			long startTime, 
			long tickDuration) {
		_actionCollectorId = actionCollectorId;
		_recvHeader = Objects.requireNonNull(recvHeader);
		_actionProcessingLogic = new ActionProcessingLogic(application, tickDuration, startTime);
		_tickDelegate = Objects.requireNonNull(tickDelegate);
		_sendQueue = Objects.requireNonNull(sendQueue);
	}
	
	@Override
	public void onEvent(TBuffer event, long sequence, boolean endOfBatch)
			throws Exception {
		if (!EventHeader.isValid(event, 0)) {
			return;
		}
		if (_recvHeader.isMessagingEvent(event)) {
			_sendQueue.send(event, _recvHeader);
			return;
		}
		
		int eventTypeId = EventPattern.getEventType(event, _recvHeader);
		if (eventTypeId == DataType.CLIENT_HANDLER_INPUT_EVENT.getId()) {
			final SocketIdentity sender = RouterPattern.getSocketId(event, _recvHeader);
			EventPattern.readContent(event, _recvHeader, _clientHandlerInputEvent, new EventReader<IncomingEventHeader, ClientHandlerInputEvent>() {

				@Override
				public void read(IncomingEventHeader header, ClientHandlerInputEvent event) {
					processAction(sender, event);
				}
				
			});
		} else if (eventTypeId == DataType.TICK_EVENT.getId()) {
			EventPattern.readContent(event, _recvHeader, _tickEvent, new EventReader<IncomingEventHeader, TickEvent>() {

				@Override
				public void read(IncomingEventHeader header, TickEvent event) {
					onTick(event);
				}
				
			});
		} else {
			Log.warn(String.format("Unknown event type %d received. Contents = %s", eventTypeId, event.toString()));
		}
	}
	
	private void processAction(SocketIdentity sender, ClientHandlerInputEvent clientHandlerInputEvent) {
		int clientHandlerId = clientHandlerInputEvent.getClientHandlerId();
		StructuredSlidingWindowMap<ResizingBuffer> reliableEventMap = _clientHandlerReliableEventStoreLookup.get(clientHandlerId);
		if (reliableEventMap == null) {
			reliableEventMap = newClientHandlerReliableEventMap();
			_clientHandlerReliableEventStoreLookup.put(clientHandlerId, reliableEventMap);
		}
		
		if (clientHandlerInputEvent.hasAction()) {
			final long reliableSeq = reliableEventMap.advance();
			
			_actionEvent.attachToBuffer(clientHandlerInputEvent.getActionSlice());
			try {
				final long effectsStartTime = _actionProcessingLogic.nextTickTime();
				final long clientId = _actionEvent.getClientIdBits();
				final long actionId = _actionEvent.getActionId();
				int actionTypeId = _actionEvent.getActionTypeId();
				ResizingBuffer actionData = _actionEvent.getActionDataSlice();
				final Iterator<ResizingBuffer> effectsIterator = _actionProcessingLogic.newAction(clientId, actionTypeId, actionData);
				
				// write the action receipt into the buffer
				_actionReceiptEvent.attachToBuffer(reliableEventMap.get(reliableSeq));
				try {
					_actionReceiptEvent.writeTypeId();
					_actionReceiptEvent.setActionCollectorId(_actionCollectorId);
					_actionReceiptEvent.setReliableSeq(reliableSeq);
					_actionReceiptEvent.setClientIdBits(clientId);
					_actionReceiptEvent.setActionId(actionId);
					_actionReceiptEvent.setStartTime(effectsStartTime);
					
					ChunkWriter effectsWriter = _actionReceiptEvent.getEffectsWriter();
					while(effectsIterator.hasNext()) {
						ResizingBuffer nextEffectBuffer = effectsIterator.next();
						nextEffectBuffer.copyTo(effectsWriter.getChunkBuffer());
						effectsWriter.commitChunk();
					}
					effectsWriter.finish();
				} finally {
					_actionReceiptEvent.releaseBuffer();
				}
				
			} finally {
				_actionEvent.releaseBuffer();
			}
		}
		
		long ackSeq = clientHandlerInputEvent.getReliableSeqAck();
		
		long headSeq = reliableEventMap.getHeadIndex();
		long tailSeq = headSeq - reliableEventMap.getLength() + 1;
		if (ackSeq + 1 < tailSeq) {
			Log.warn(String.format("Client Handler %d: requested %d, but %d " +
					"was the lowest seq available", clientHandlerId, ackSeq + 1, tailSeq));
		}
		
		// send action receipts
		long startSeq = Math.max(ackSeq + 1, tailSeq);
		for (long seq = startSeq; seq <= headSeq; seq++) {
			final ResizingBuffer receiptBuffer = reliableEventMap.get(seq);
			_sendQueue.send(RouterPattern.asReliableTask(sender, _actionReceiptEvent, new EventWriter<OutgoingEventHeader, ActionReceiptEvent>() {

				@Override
				public void write(OutgoingEventHeader header, ActionReceiptEvent event)
						throws Exception {
					receiptBuffer.copyTo(event.getBuffer());
				}
			}));
		}
	}
	
	private void onTick(TickEvent tickEvent) {
		long time = tickEvent.getTime();
		_tickDelegate.onTick(time, _actionProcessingLogic.tick(time));
	}
	
	private static StructuredSlidingWindowMap<ResizingBuffer> newClientHandlerReliableEventMap() {
		return new StructuredSlidingWindowMap<ResizingBuffer>(
					128, 
					ResizingBuffer.class, 
					new ComponentFactory<ResizingBuffer>() {

						@Override
						public ResizingBuffer newInstance(Object[] initArgs) {
							return new ArrayBackedResizingBuffer(Constants.MSG_BUFFER_ENTRY_LENGTH);
						}
					}, 
					new InitialiseDelegate<ResizingBuffer>() {
						
						@Override
						public void initialise(ResizingBuffer content) {
							content.reset();
						}
					});	
	}

}
