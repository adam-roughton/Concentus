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
package com.adamroughton.concentus.crowdhammer.worker;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import uk.co.real_logic.intrinsics.ComponentFactory;


import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.InitialiseDelegate;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientConnectEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientUpdateEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ConnectResponseEvent;
import com.adamroughton.concentus.data.model.bufferbacked.ActionReceipt;
import com.adamroughton.concentus.data.model.bufferbacked.BufferBackedEffect;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.RouterPattern;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.metric.CountMetric;
import com.adamroughton.concentus.metric.StatsMetric;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.util.IntArray;

import static com.adamroughton.concentus.Constants.TIME_STEP_IN_MS;

public final class Client {

	/**
	 * Buffer 10 seconds worth of sent actions or received update time stamps, count as no response
	 * if not received within this window.
	 */
	public final static int WINDOW_SIZE = Util.nextPowerOf2((int)(10000 / TIME_STEP_IN_MS));	
	private static final long CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);	
	
	private final StructuredSlidingWindowMap<SentActionInfo> _actionIdToSentActionInfoLookup = 
			new StructuredSlidingWindowMap<>(WINDOW_SIZE, SentActionInfo.class, 
		new ComponentFactory<SentActionInfo>() {

			@Override
			public SentActionInfo newInstance(Object[] initArgs) {
				return new SentActionInfo();
			}
		
		}, new InitialiseDelegate<SentActionInfo>() {

			@Override
			public void initialise(SentActionInfo sentActionInfo) {
				sentActionInfo.sentTime = -1;
				sentActionInfo.startTime = -1;
			}
			
		});
	
	private final ClientAgent _agent;
	private long _simTime = 0;
		
	private final long _index;
	private final Clock _clock;
	
	private final ClientConnectEvent _connectEvent = new ClientConnectEvent();
	private final ClientInputEvent _inputEvent = new ClientInputEvent();
	private final ActionEvent _actionEvent = new ActionEvent();
	
	private final CanonicalStateUpdate _canonicalStateUpdate = new CanonicalStateUpdate();
	private final BufferBackedEffect _effect = new BufferBackedEffect();
	private final ActionReceipt _receipt = new ActionReceipt();
	
	private CountMetric _connectedClientCountMetric;
	private CountMetric _sentActionThroughputMetric;
	private StatsMetric _actionToCanonicalStateLatencyMetric;
	private CountMetric _lateActionToCanonicalStateCountMetric;	
	private CountMetric _droppedActionThroughputMetric;
	
	private long _connectReqSendTime = -1;
	
	//private final long[] _neighbourJointActionIds = new long[25];
	
	private long _lastActionTime = -1;
	private long _reliableSeq = -1;
	
	private long _clientId = -1;
	private byte[] _handlerId = new byte[0];
	
	private boolean _isActive = false;
	private boolean _isConnecting = false;
	
	public Client(long index, Clock clock, ClientAgent agent) {
		_index = index;
		_clock = Objects.requireNonNull(clock);
		_agent = Objects.requireNonNull(agent);
	}
	
	public void setMetricCollectors(
			CountMetric connectedClientCountMetric,
			CountMetric sentActionThroughputMetric,
			StatsMetric actionToCanonicalStateLatencyMetric,
			CountMetric lateActionToCanonicalStateCountMetric,	
			CountMetric droppedActionThroughputMetric) {
		_connectedClientCountMetric = connectedClientCountMetric;
		_sentActionThroughputMetric = sentActionThroughputMetric;
		_actionToCanonicalStateLatencyMetric = actionToCanonicalStateLatencyMetric;
		_lateActionToCanonicalStateCountMetric = lateActionToCanonicalStateCountMetric;
		_droppedActionThroughputMetric = droppedActionThroughputMetric;
	}
	
	public void unsetMetricCollectors() {
		_connectedClientCountMetric = null;
		_sentActionThroughputMetric = null;
		_actionToCanonicalStateLatencyMetric = null;
		_lateActionToCanonicalStateCountMetric = null;
		_droppedActionThroughputMetric = null;
	}
	
	public boolean isActive() {
		return _isActive;
	}
	
	public void setIsActive(boolean isActive) {
		_isActive = isActive;
	}
	
	public byte[] getHandlerId() {
		return _handlerId;
	}
	
	public void setHandlerId(byte[] handlerId) {
		_handlerId = handlerId;
	}
	
	public long getNextDeadline() {
		if (!hasConnected()) {
			return _clock.currentMillis();
		} else {
			return _lastActionTime + TIME_STEP_IN_MS;
		}
	}
	
	public long getClientId() {
		return _clientId;
	}
	
	private boolean hasConnected() {
		return _clientId != -1;
	}
	
	public <TBuffer extends ResizingBuffer> void onActionDeadline(SendQueue<OutgoingEventHeader, TBuffer> clientSendQueue) {
		if (_clientId == -1) {
			if (_isConnecting) {
				// if we are waiting to connect and still within the timeout, do nothing with this client
				if (_clock.currentMillis() - _connectReqSendTime < CONNECT_TIMEOUT)	return;
			}
			connect(clientSendQueue);
			_isConnecting = true;
		} else {
			sendInputAction(clientSendQueue);
		}
		_lastActionTime = _clock.currentMillis();
	}
	
	private <TBuffer extends ResizingBuffer> void sendInputAction(SendQueue<OutgoingEventHeader, TBuffer> clientSendQueue) {		
		if (!clientSendQueue.trySend(RouterPattern.asTask(_handlerId, false, _inputEvent, new EventWriter<OutgoingEventHeader, ClientInputEvent>() {

			@Override
			public void write(OutgoingEventHeader header, ClientInputEvent event) throws Exception {
				long sendTime = _clock.currentMillis();
				long actionId = nextActionId(sendTime);
				event.setClientId(_clientId);
				event.setReliableSeqAck(_reliableSeq);
				_actionEvent.attachToBuffer(event.getActionSlice());
				_actionEvent.writeTypeId();
				_actionEvent.setActionId(actionId);
				_actionEvent.setClientIdBits(_clientId);
				event.setHasAction(_agent.onInputGeneration(_actionEvent));
				_actionEvent.releaseBuffer();
			}
			
		}))) {
			_droppedActionThroughputMetric.push(1);
		}
		_sentActionThroughputMetric.push(1);
	}
	
	private <TBuffer extends ResizingBuffer> void connect(SendQueue<OutgoingEventHeader, TBuffer> clientSendQueue) {
		clientSendQueue.send(RouterPattern.asTask(_handlerId, false, _connectEvent, new EventWriter<OutgoingEventHeader, ClientConnectEvent>() {

			@Override
			public void write(OutgoingEventHeader header, ClientConnectEvent event) throws Exception {
				event.setCallbackBits(_index);
			}
			
		}));
		_connectReqSendTime = _clock.currentMillis();
	}
	
//	private long _windowCursor = 0;
//	private ArrayBackedResizingBuffer[] _updateWindow = new ArrayBackedResizingBuffer[5];
//	{
//		for (int i = 0; i < _updateWindow.length; i++) {
//			_updateWindow[i] = new ArrayBackedResizingBuffer(512);
//		}
//	}
	
	public void onClientUpdate(ClientUpdateEvent updateEvent) {
		long updateRecvTime = _clock.currentMillis();
		
		//ResizingBuffer cachedBuffer = _updateWindow[(int)(_windowCursor++ % _updateWindow.length)];
		//cachedBuffer.reset();
		//updateEvent.getBuffer().copyTo(cachedBuffer);

		IntArray intArray = new IntArray();
		if (updateEvent.getClientId() != _clientId) {
			throw new RuntimeException("ClientUpdate received by the wrong client! " +
					"(got update for " + updateEvent.getClientId() + " in " + _clientId);
		}
		
		try {
			for (byte[] chunk : updateEvent.getChunkedContent()) {
				intArray.add(chunk.length);
				ArrayBackedResizingBuffer chunkBuffer = new ArrayBackedResizingBuffer(chunk);
				int cursor = 0;
				long reliableSeq = chunkBuffer.readLong(cursor);
				cursor += ResizingBuffer.LONG_SIZE;
				
				// only process chunk if unreliable (-1) or if all previous
				// reliable chunks have been processed
				if (reliableSeq == -1 || reliableSeq == _reliableSeq + 1) {
					int dataType = chunkBuffer.readInt(cursor);
					
					if (dataType == _receipt.getTypeId()) {
						_receipt.attachToBuffer(chunkBuffer, cursor);
						try {
							onActionReceipt(_receipt, updateRecvTime);
						} finally {
							_receipt.releaseBuffer();
						}
					} else if (dataType == _canonicalStateUpdate.getTypeId()) {
						_canonicalStateUpdate.attachToBuffer(chunkBuffer, cursor);
						try {
							onCanonicalStateUpdate(_canonicalStateUpdate, updateRecvTime);
						} finally {
							_canonicalStateUpdate.releaseBuffer();
						}
					} else if (dataType == _effect.getTypeId()) {
						_effect.attachToBuffer(chunkBuffer, cursor);
						try {
							onEffectUpdate(_effect, updateRecvTime);
						} finally {
							_effect.releaseBuffer();
						}
					}
					
					if (reliableSeq >= 0) {
						_reliableSeq = reliableSeq;
					}
				}
			}
		} catch (RuntimeException eRuntime) {
//			Log.error("Error on update for client: " + _clientId);
//			Log.error("Last valid chunk lengths: " + intArray.toString());
//			Log.error("Last updates recved: ");
//			for (long cursor = _windowCursor - 1; cursor >= Math.max(0, _windowCursor - _updateWindow.length); cursor--) {
//				ResizingBuffer cachedUpdateBuffer = _updateWindow[(int)(cursor % _updateWindow.length)];
//				Log.info("Update: (content length = " + cachedUpdateBuffer.getContentSize() + ") " + cachedUpdateBuffer.toString() + '\n');
//			}
			throw eRuntime;
		}
	}
	
	private long nextActionId(long sendTime) {
		/* 
		 * if any actions drop out of the sliding window, they are
		 * considered unacknowledged
		 */
		long prevTailId = _actionIdToSentActionInfoLookup.getHeadIndex() - _actionIdToSentActionInfoLookup.getLength() + 1;
		if (_actionIdToSentActionInfoLookup.containsIndex(prevTailId)) {
			_lateActionToCanonicalStateCountMetric.push(1);
		}
		long nextActionId = _actionIdToSentActionInfoLookup.advance();
		_actionIdToSentActionInfoLookup.get(nextActionId).sentTime = sendTime;
		
		return nextActionId;
	}
	
	private void onActionReceipt(ActionReceipt receipt, long recvTime) {
		long actionId = receipt.getActionId();
		long startTime = receipt.getStartTime();
		if (_actionIdToSentActionInfoLookup.containsIndex(actionId)) {
			SentActionInfo sentAction = _actionIdToSentActionInfoLookup.get(actionId);
			if (_simTime < startTime) {
				sentAction.startTime = startTime;
			} else {
				_actionToCanonicalStateLatencyMetric.push(recvTime - sentAction.sentTime);
				_actionIdToSentActionInfoLookup.remove(actionId);
			}
		}
	}
	
	private void onCanonicalStateUpdate(CanonicalStateUpdate update, long recvTime) {
		// search through pending actions to work out latency
		_simTime = Math.max(_simTime, update.getTime());
		long headId = _actionIdToSentActionInfoLookup.getHeadIndex();
		long tailId = headId - _actionIdToSentActionInfoLookup.getLength() + 1;
		for (long actionId = tailId; actionId <= headId; actionId++) {
			if (_actionIdToSentActionInfoLookup.containsIndex(actionId)) {
				SentActionInfo sentAction = _actionIdToSentActionInfoLookup.get(actionId);
				long startTime = sentAction.startTime;
				if (startTime != -1 && startTime <= _simTime) {
					_actionToCanonicalStateLatencyMetric.push(recvTime - sentAction.sentTime);
					_actionIdToSentActionInfoLookup.remove(actionId);
				}
			}
		}
		_agent.onUpdate(update);
	}
	
	private void onEffectUpdate(BufferBackedEffect effect, long recvTime) {
	}
	
	public void onConnectResponse(ConnectResponseEvent connectResEvent) {
		if (!_isConnecting) 
			throw new RuntimeException(String.format("Expected the client (index = %d) to be connecting on reception of a connect response event.", _index));
		if (connectResEvent.getResponseCode() != ConnectResponseEvent.RES_OK) {
			throw new RuntimeException(String.format("The response code for a client connection was %d, expected %d (OK). Aborting test", 
					connectResEvent.getResponseCode(), ConnectResponseEvent.RES_OK));
		}
		_clientId = connectResEvent.getClientIdBits();
		_agent.setClientId(_clientId);
		_isConnecting = false;
		_connectedClientCountMetric.push(1);
	}
	
	public void reset() {
		_isConnecting = false;
		_clientId = -1;
		_isActive = false;
		_actionIdToSentActionInfoLookup.clear();
		_simTime = -1;
		unsetMetricCollectors();
	}
	
	private static class SentActionInfo {
		public long sentTime;
		public long startTime;
	}

}
