package com.adamroughton.concentus.actioncollector;

import java.util.Iterator;
import java.util.Objects;

import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ActionReceiptEvent;
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
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;

public final class ActionCollectorProcessor<TBuffer extends ResizingBuffer> implements EventHandler<TBuffer> {

	private final ActionProcessingLogic _actionProcessingLogic;
	private final TickDelegate _tickDelegate;
	private final SendQueue<OutgoingEventHeader, TBuffer> _sendQueue;
	private final IncomingEventHeader _recvHeader;
	
	private final ActionEvent _actionEvent = new ActionEvent();
	private final TickEvent _tickEvent = new TickEvent();
	private final ActionReceiptEvent _actionReceiptEvent = new ActionReceiptEvent();
	
	public ActionCollectorProcessor(
			IncomingEventHeader recvHeader,
			CollectiveApplication application, 
			TickDelegate tickDelegate, 
			SendQueue<OutgoingEventHeader, TBuffer> sendQueue,
			long startTime, 
			long tickDuration) {
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
		if (eventTypeId == DataType.ACTION_EVENT.getId()) {
			final SocketIdentity sender = RouterPattern.getSocketId(event, _recvHeader);
			EventPattern.readContent(event, _recvHeader, _actionEvent, new EventReader<IncomingEventHeader, ActionEvent>() {

				@Override
				public void read(IncomingEventHeader header, ActionEvent event) {
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
	
	private void processAction(SocketIdentity sender, ActionEvent actionEvent) {
		final long effectsStartTime = _actionProcessingLogic.nextTickTime();
		final long clientId = actionEvent.getClientIdBits();
		final long actionId = actionEvent.getActionId();
		int actionTypeId = actionEvent.getActionTypeId();
		ResizingBuffer actionData = actionEvent.getActionDataSlice();
		final Iterator<ResizingBuffer> effectsIterator = _actionProcessingLogic.newAction(clientId, actionTypeId, actionData);
		
		_sendQueue.send(RouterPattern.asReliableTask(sender, _actionReceiptEvent, new EventWriter<OutgoingEventHeader, ActionReceiptEvent>() {

			@Override
			public void write(OutgoingEventHeader header, ActionReceiptEvent event)
					throws Exception {
				event.setClientIdBits(clientId);
				event.setActionId(actionId);
				event.setStartTime(effectsStartTime);
				
				ChunkWriter effectsWriter = event.getEffectsWriter();
				while(effectsIterator.hasNext()) {
					ResizingBuffer nextEffectBuffer = effectsIterator.next();
					nextEffectBuffer.copyTo(effectsWriter.getChunkBuffer());
					effectsWriter.commitChunk();
				}
				effectsWriter.finish();
			}
		}));
	}
	
	private void onTick(TickEvent tickEvent) {
		long time = tickEvent.getTime();
		_tickDelegate.onTick(time, _actionProcessingLogic.tick(time));
	}

}
