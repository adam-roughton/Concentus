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
import com.adamroughton.concentus.data.NullResizingBuffer;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ActionReceiptEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ActionReceiptMetaData;
import com.adamroughton.concentus.data.events.bufferbacked.ClientHandlerInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientHandlerUpdateEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ReplayRequestEvent;
import com.adamroughton.concentus.data.events.bufferbacked.TickEvent;
import com.adamroughton.concentus.data.model.bufferbacked.BufferBackedEffect;
import com.adamroughton.concentus.data.model.kryo.CandidateValue;
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
	
	private final TickEvent _tickEvent = new TickEvent();
	private final ClientHandlerInputEvent _clientHandlerInputEvent = new ClientHandlerInputEvent();
	
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
					processClientHandlerInputEvent(sender, event);
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
	
	private void processClientHandlerInputEvent(SocketIdentity sender, final ClientHandlerInputEvent clientHandlerInputEvent) {
		final int clientHandlerId = clientHandlerInputEvent.getClientHandlerId();
		
		final StructuredSlidingWindowMap<ResizingBuffer> reliableEventMap;
		if (_clientHandlerReliableEventStoreLookup.containsKey(clientHandlerId)) {
			reliableEventMap = _clientHandlerReliableEventStoreLookup.get(clientHandlerId);
		} else {
			reliableEventMap = newClientHandlerReliableEventMap();
			_clientHandlerReliableEventStoreLookup.put(clientHandlerId, reliableEventMap);
		}
		
		final int handlerEventTypeId = clientHandlerInputEvent.getContentSlice().readInt(0);
		final long nextReliableSeq = reliableEventMap.advance();
		_sendQueue.send(RouterPattern.asReliableTask(sender, new ClientHandlerUpdateEvent(), new EventWriter<OutgoingEventHeader, ClientHandlerUpdateEvent>() {

			@Override
			public void write(OutgoingEventHeader header,
					ClientHandlerUpdateEvent update) throws Exception {
				update.setClientHandlerId(clientHandlerId);
				update.setReliableSeq(nextReliableSeq);
				update.setActionCollectorId(_actionCollectorId);
				
				ChunkWriter updateChunkWriter = update.newChunkedContentWriter();
				
				ResizingBuffer reliableDataBuffer = reliableEventMap.get(nextReliableSeq);
				if (handlerEventTypeId == DataType.ACTION_EVENT.getId()) {
					ActionEvent actionEvent = new ActionEvent();
					
					actionEvent.attachToBuffer(clientHandlerInputEvent.getContentSlice());
					
					ActionReceiptMetaData actionReceiptMetaData = new ActionReceiptMetaData();
					actionReceiptMetaData.attachToBuffer(reliableDataBuffer);
					try {
						actionReceiptMetaData.writeTypeId();
						processAction(actionEvent, actionReceiptMetaData, updateChunkWriter);
					} finally {
						actionEvent.releaseBuffer();
						actionReceiptMetaData.releaseBuffer();
					}
				} else if (handlerEventTypeId == DataType.REPLAY_REQUEST_EVENT.getId()) {
					ReplayRequestEvent replayRequestEvent = new ReplayRequestEvent();
					replayRequestEvent.attachToBuffer(clientHandlerInputEvent.getContentSlice());
					try {
						processReplayRequest(clientHandlerId, replayRequestEvent, reliableEventMap, 
								reliableDataBuffer, updateChunkWriter);
					} finally {
						replayRequestEvent.releaseBuffer();
					}
				} else {
					Log.warn("ActionCollectorProcessor.processClientHandlerInputEvent: Unknown event type ID " + handlerEventTypeId);
				}
			}
			
		}));
	}
	
	private void processAction(ActionEvent actionEvent,
			ActionReceiptMetaData receiptMetaData, 
			ChunkWriter updateChunkWriter) {			
		final long effectsStartTime = _actionProcessingLogic.nextTickTime();
		final long clientId = actionEvent.getClientIdBits();
		final long actionId = actionEvent.getActionId();
		int actionTypeId = actionEvent.getActionTypeId();
		ResizingBuffer actionData = actionEvent.getActionDataSlice();
		final Iterator<ResizingBuffer> effectsIterator = _actionProcessingLogic.newAction(clientId, actionTypeId, actionData);
		
		ResizingBuffer chunkBuffer = updateChunkWriter.getChunkBuffer();
		ActionReceiptEvent actionReceiptEvent = new ActionReceiptEvent();
		actionReceiptEvent.attachToBuffer(chunkBuffer);
		try {
			// write the action receipt into the buffer
			actionReceiptEvent.writeTypeId();
			actionReceiptEvent.setClientIdBits(clientId);
			actionReceiptEvent.setActionId(actionId);
			actionReceiptEvent.setStartTime(effectsStartTime);
			
			receiptMetaData.setClientIdBits(clientId);
			int varIdCount = 0;
			ResizingBuffer varIdsBuffer = receiptMetaData.getVarIdsSlice();
			int varIdsCursor = 0;
			
			ChunkWriter effectsWriter = actionReceiptEvent.getEffectsWriter();
			while(effectsIterator.hasNext()) {
				ResizingBuffer nextEffectBuffer = effectsIterator.next();
				nextEffectBuffer.copyTo(effectsWriter.getChunkBuffer());
				effectsWriter.commitChunk();
				
				BufferBackedEffect effect = new BufferBackedEffect();
				effect.attachToBuffer(nextEffectBuffer);
				try {
					varIdsBuffer.writeInt(varIdsCursor, effect.getVariableId());
					varIdsCursor += ResizingBuffer.INT_SIZE;
					varIdCount++;
				} finally {
					effect.releaseBuffer();
				}
			}
			effectsWriter.finish();
			
			receiptMetaData.setVarIdCount(varIdCount);
		} finally {
			actionReceiptEvent.releaseBuffer();
			updateChunkWriter.commitChunk();
		}
	}
	
	private void processReplayRequest(
			int clientHandlerId,
			ReplayRequestEvent replayRequest, 
			StructuredSlidingWindowMap<ResizingBuffer> reliableEventWindow, 
			ResizingBuffer thisRequestReliableBuffer,
			ChunkWriter updateChunkWriter) {
		// store a copy of the request into the reliable buffer
		replayRequest.getBuffer().copyTo(thisRequestReliableBuffer);
		
		long headSeq = reliableEventWindow.getHeadIndex();
		long tailSeq = headSeq - reliableEventWindow.getLength() + 1;
		
		long requestedStartSeq = replayRequest.getStartSequence();
		long startSeq = Math.max(tailSeq, requestedStartSeq);
		long endSeq = Math.min(headSeq, startSeq + replayRequest.getCount());
		
		if (startSeq > requestedStartSeq) {
			Log.warn(String.format("Client Handler %d: requested %d, but %d " +
					"was the lowest seq available", clientHandlerId, requestedStartSeq, startSeq));
		}
		
		for (long seq = startSeq; seq <= endSeq; seq++) {
			ResizingBuffer receiptChunkBuffer = updateChunkWriter.getChunkBuffer();			
			final ResizingBuffer reliableEventBuffer = reliableEventWindow.get(seq);
			
			int storedEventTypeId = reliableEventBuffer.readInt(0);
			if (storedEventTypeId == DataType.ACTION_RECEIPT_META_DATA.getId()) {
				ActionReceiptEvent actionReceiptEvent = new ActionReceiptEvent();
				actionReceiptEvent.attachToBuffer(receiptChunkBuffer);
				
				ActionReceiptMetaData actionReceiptMetaData = new ActionReceiptMetaData();
				actionReceiptMetaData.attachToBuffer(reliableEventBuffer);
				try {
					long clientIdBits = actionReceiptMetaData.getClientIdBits();
					actionReceiptEvent.writeTypeId();
					actionReceiptEvent.setActionId(actionReceiptMetaData.getActionId());
					actionReceiptEvent.setClientIdBits(clientIdBits);
					actionReceiptEvent.setStartTime(actionReceiptMetaData.getStartTime());
					
					ResizingBuffer varIdsBuffer = actionReceiptMetaData.getVarIdsSlice();
					int varsIdCount = actionReceiptMetaData.getVarIdCount();
					
					ChunkWriter effectChunkWriter = actionReceiptEvent.getEffectsWriter();
					for (int i = 0; i < varsIdCount; i++) {
						int varId = varIdsBuffer.readInt(i * ResizingBuffer.INT_SIZE);
						ResizingBuffer effectData = _actionProcessingLogic.getEffectData(clientIdBits, varId);
						if (effectData.getContentSize() > 0) {
							effectData.copyTo(effectChunkWriter.getChunkBuffer());
							effectChunkWriter.commitChunk();								
						}
					}
				} finally {
					actionReceiptMetaData.releaseBuffer();
					actionReceiptEvent.releaseBuffer();
				}
			} else if (storedEventTypeId == DataType.REPLAY_REQUEST_EVENT.getId()) {
				ReplayRequestEvent lostReplayRequestEvent = new ReplayRequestEvent();
				lostReplayRequestEvent.attachToBuffer(receiptChunkBuffer);
				try {
					processReplayRequest(clientHandlerId, lostReplayRequestEvent, reliableEventWindow, new NullResizingBuffer(), updateChunkWriter);
				} finally {
					lostReplayRequestEvent.releaseBuffer();
				}
			}
		}
		
	}
	
	private void onTick(TickEvent tickEvent) {
		long time = tickEvent.getTime();
		Iterator<CandidateValue> candidateValuesForTick = _actionProcessingLogic.tick(time);
		_tickDelegate.onTick(time, candidateValuesForTick);
	}
	
	private static StructuredSlidingWindowMap<ResizingBuffer> newClientHandlerReliableEventMap() {
		return new StructuredSlidingWindowMap<ResizingBuffer>(
					4096, 
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
