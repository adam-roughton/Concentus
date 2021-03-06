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
package com.adamroughton.concentus.clienthandler;

import org.javatuples.Pair;

import uk.co.real_logic.intrinsics.ComponentFactory;

import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ChunkWriter;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.events.bufferbacked.ActionReceiptEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientInputEvent;
import com.adamroughton.concentus.data.events.bufferbacked.ClientUpdateEvent;
import com.adamroughton.concentus.data.model.ClientId;
import com.adamroughton.concentus.data.model.bufferbacked.ActionReceipt;
import com.adamroughton.concentus.data.model.bufferbacked.CanonicalStateUpdate;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.util.SlidingWindowLongMap;
import com.adamroughton.concentus.util.StructuredSlidingWindowMap;
import com.adamroughton.concentus.InitialiseDelegate;
import com.esotericsoftware.minlog.Log;

public final class ClientProxy {

	private final ClientId _clientId;
	private final long _clientIdBits;
	private SocketIdentity _clientRef;
	private int _actionCollectorId;
	private SocketIdentity _actionCollectorRef;
	private long _lastMsgTime;
	private long _lastCanonicalStateUpdateId;
	
	private boolean _isActive = false;
	private boolean _shouldDropFlag = false;
	
	private final SlidingWindowLongMap _recvActionMap = 
			new SlidingWindowLongMap(ClientUpdateEvent.ACK_FIELD_LENGTH * 8 * 2);
	
	private final StructuredSlidingWindowMap<ResizingBuffer> _reliableDataMap = 
			new StructuredSlidingWindowMap<ResizingBuffer>(
					128, 
					ResizingBuffer.class, 
					new ComponentFactory<ResizingBuffer>() {

						@Override
						public ResizingBuffer newInstance(Object[] initArgs) {
							return new ArrayBackedResizingBuffer(128);
						}
					}, 
					new InitialiseDelegate<ResizingBuffer>() {
						
						@Override
						public void initialise(ResizingBuffer content) {
							content.reset();
						}
					});
	
	private final ActionReceipt _actionReceiptData = new ActionReceipt();
	
	public ClientProxy(ClientId clientId) {
		_clientId = clientId;
		_clientIdBits = clientId.toBits();
		_lastMsgTime = 0;
		_clientRef = new SocketIdentity(new byte[0]);
		_actionCollectorRef = new SocketIdentity(new byte[0]);
		_lastCanonicalStateUpdateId = -1;
		_isActive = false;
	}
	
	public boolean isActive() {
		return _isActive;
	}
	
	public void setIsActive(boolean isActive) {
		_isActive = isActive;
	}
	
	public boolean shouldDrop() {
		return _shouldDropFlag;
	}
	
	public void clear() {
		_reliableDataMap.clear();
		_clientRef = new SocketIdentity(new byte[0]);
		_actionCollectorRef = new SocketIdentity(new byte[0]);
		_clientRef = new SocketIdentity(new byte[0]);
		_isActive = false;
		_shouldDropFlag = false;
	}
	
	public ClientId getClientId() {
		return _clientId;
	}
	
	public long getClientIdBits() {
		return _clientIdBits;
	}
	
	public void setClientRef(SocketIdentity clientRef) {
		_clientRef = clientRef;
	}
	
	public SocketIdentity getClientSocketId() {
		return _clientRef;
	}
	
	public void setActionCollectorAllocation(Pair<Integer, SocketIdentity> actionCollectorRef) {
		_actionCollectorId = actionCollectorRef.getValue0();
		_actionCollectorRef = actionCollectorRef.getValue1();
	}
	
	public int getActionCollectorId() {
		return _actionCollectorId;
	}
	
	public SocketIdentity getActionCollectorRef() {
		return _actionCollectorRef;
	}
	
	public long getLastMsgTime() {
		return _lastMsgTime;
	}
	
	public void setLastMsgTime(final long msgTime) {
		_lastMsgTime = msgTime;
	}
	
	public long getLastCanonicalStateUpdateId() {
		return _lastCanonicalStateUpdateId;
	}
	
	public void setLastCanonicalStateUpdateId(final long canonicalStateUpdateId) {
		_lastCanonicalStateUpdateId = canonicalStateUpdateId;
	}
	
	/**
	 * Notes that this action has been received, returning {@code true}
	 * if this action ID has not been seen before, {@code false} otherwise.
	 * @param actionId
	 * @return {@code true} if this action ID has not been seen before, {@code false} otherwise
	 */
	public boolean isNewAction(long actionId) {
		long headId = _recvActionMap.getHeadIndex();
		if (actionId > headId) {
			_recvActionMap.put(actionId, 1);
			return true;
		} else if (actionId > headId - _recvActionMap.getLength()) {
			boolean isNewAction;
			if (_recvActionMap.containsIndex(actionId)) {
				long flag = _recvActionMap.getDirect(actionId);
				isNewAction = flag == 0;
			} else {
				isNewAction = true;
			}
			_recvActionMap.put(actionId, 1);
			return isNewAction;
		} else {
			return false;
		}
	}
	
	public void generateUpdate(ClientInputEvent inputEvent,
			CanonicalStateUpdate latestCanonicalState,
			ClientUpdateEvent updateEvent) { 
		long ackSeq = inputEvent.getReliableSeqAck();
		
		long headSeq = _reliableDataMap.getHeadIndex();
		long tailSeq = headSeq - _reliableDataMap.getLength() + 1;
		if (ackSeq + 1 < tailSeq) {
			// signal that the client has been disconnected
			Log.info("Client.generateUpdate: " + inputEvent);
			Log.warn(String.format("Disconnecting client %d: requested %d, but %d " +
					"was the lowest seq available", _clientIdBits, ackSeq + 1, tailSeq));
			_shouldDropFlag = true;
			return;
		}
		
		// set action ACK flags
		long headActionId = _recvActionMap.getHeadIndex();
		long ackFieldLength = updateEvent.getAckFieldLength();
		updateEvent.setActionAckFlagsHeadId(headActionId);
		for (int i = 0; i < ackFieldLength; i++) {
			long actionId = headActionId - ackFieldLength + i;
			if (actionId >= 0) {
				boolean nackFlag = !_recvActionMap.containsIndex(actionId) || _recvActionMap.get(actionId) == 0;
				updateEvent.setNackFlagAtIndex(i, nackFlag);
			}
		}

		ChunkWriter updateChunkWriter = updateEvent.newChunkedContentWriter();
		ResizingBuffer chunkBuffer = updateChunkWriter.getChunkBuffer();
		
		// write canonical state update if there is a newer copy
		long latestCanonicalStateId = latestCanonicalState.getUpdateId();
		if (latestCanonicalStateId > _lastCanonicalStateUpdateId) {
			ResizingBuffer latestCanonicalStateBuffer = latestCanonicalState.getBuffer();
			int canonicalStateSize = latestCanonicalStateBuffer.getContentSize();
			
			// signal unreliable chunk
			chunkBuffer.writeLong(0, -1);
			
			// copy update data
			latestCanonicalStateBuffer.copyTo(chunkBuffer, ResizingBuffer.LONG_SIZE, canonicalStateSize);
			updateChunkWriter.commitChunk();
			_lastCanonicalStateUpdateId = latestCanonicalStateId;
		}
		
		// write reliable chunks
		for (long seq = ackSeq + 1; seq <= headSeq; seq++) {
			chunkBuffer.writeLong(0, seq);
			ResizingBuffer reliableChunk = _reliableDataMap.get(seq);
			int chunkLength = reliableChunk.getContentSize();
			reliableChunk.copyTo(chunkBuffer, ResizingBuffer.LONG_SIZE, chunkLength);
			updateChunkWriter.commitChunk();
		}
		
		updateChunkWriter.finish();	
	}
	
	public void processActionReceipt(ActionReceiptEvent actionReceiptEvent) {
		if (actionReceiptEvent.getClientIdBits() != _clientIdBits) {
			throw new RuntimeException("ActionReceipt sent to the wrong client proxy! " +
					"(got receipt for client " + ClientId.fromBits(actionReceiptEvent.getClientIdBits()) + " in " + _clientId);
		}
		
		// create actionReceipt
		long seq = _reliableDataMap.advance();
		_actionReceiptData.attachToBuffer(_reliableDataMap.get(seq));
		try {
			_actionReceiptData.writeTypeId();
			_actionReceiptData.setActionId(actionReceiptEvent.getActionId());
			_actionReceiptData.setStartTime(actionReceiptEvent.getStartTime());
		} finally {
			_actionReceiptData.releaseBuffer();
		}
		
		// create effects
		for (byte[] effect : actionReceiptEvent.getEffects()) {
			seq = _reliableDataMap.advance();
			_reliableDataMap.get(seq).writeBytes(0, effect);
		}
	}
	
	
}
