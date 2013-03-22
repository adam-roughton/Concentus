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
package com.adamroughton.consentus.crowdhammer.worker;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import jline.internal.Log;

import org.zeromq.ZMQ;

import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.messaging.EventProcessingHeader;
import com.adamroughton.consentus.messaging.EventReceiver;
import com.adamroughton.consentus.messaging.EventSender;
import com.adamroughton.consentus.messaging.MessageFrameBufferMapping;
import com.adamroughton.consentus.messaging.events.ClientInputEvent;
import com.adamroughton.consentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.consentus.messaging.events.EventType;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import uk.co.real_logic.intrinsics.StructuredArray;

import static com.adamroughton.consentus.Constants.*;

public class ClientReactor implements Runnable {

	private final static long METRIC_TICK_NS = TimeUnit.MILLISECONDS.toNanos(METRIC_TICK);
	
	private final AtomicBoolean _instanceActive = new AtomicBoolean(false);
	private final StructuredArray<Client> _clients;
	private final long _clientLengthMask;
	
	private volatile boolean _isRunning = false;
	private volatile boolean _isSendingInput = false;
	
	private final RingBuffer<byte[]> _metricSendQueue;
	private final SequenceBarrier _metricSendBarrier;
	
	private final EventProcessingHeader _header;
	private final EventSender _sender;
	private final EventReceiver _receiver;
	
	private final ClientInputEvent _inputEvent = new ClientInputEvent();
	private final ClientUpdateEvent _updateEvent = new ClientUpdateEvent();
	
	public ClientReactor(final StructuredArray<Client> clients, 
			final RingBuffer<byte[]> metricSendQueue,
			final SequenceBarrier metricSendBarrier) {
		_clients = Objects.requireNonNull(clients);
		long clientsLength = _clients.getLength();
		if (Long.bitCount(clientsLength) != 1) 
			throw new IllegalArgumentException("The allocated client array must be a power of 2.");
		_clientLengthMask = clientsLength - 1;
		
		_metricSendQueue = Objects.requireNonNull(metricSendQueue);
		_metricSendBarrier = Objects.requireNonNull(metricSendBarrier);
		
		_header = new EventProcessingHeader(0, 1);
		_sender = new EventSender(_header, false);
		_receiver = new EventReceiver(_header, true);
	}
	
	public void halt() {
		_isRunning = false;
	}
	
	public void stopSendingInput() {
		_isSendingInput = false;
	}
	
	@Override
	public void run() {
		if (_instanceActive.getAndSet(true)) {
			throw new IllegalArgumentException(
					"Only one instance of the client reactor should be started.");
		}
		_isRunning = true;
		_isSendingInput = true;
		
		byte[] messageBuffer = new byte[MSG_BUFFER_LENGTH];
		MessageFrameBufferMapping msgPartPolicy = new MessageFrameBufferMapping(0);
		long nextMetricTime = 0;
		
		long currentClientIndex = -1;
		long now;
		while(_isRunning) {
			now = System.nanoTime();
			if (System.nanoTime() >= nextMetricTime) {
				sendMetricEvent();
				nextMetricTime = now + METRIC_TICK_NS;
			}
			
			// measure whether we have entered death spiral with latency - check that
			// the processing time for all clients is less than the tick time
			
			// get next client
			Client client = _clients.get(++currentClientIndex & _clientLengthMask);
			if (client.isActive()) {
				long nextSendTime = client.getNextSendTimeInNanos();
				
				// recv events
				ZMQ.Socket socket = client.getSocket();
				while(_receiver.recv(socket, msgPartPolicy, 0, messageBuffer)) {
					processClientRecv(messageBuffer);
				}
				
				// send outgoing event
				if (_isSendingInput) {
					LockSupport.parkNanos(getWaitTime(nextSendTime));
					createClientEvent(client, messageBuffer);
					if (!_sender.send(socket, msgPartPolicy, messageBuffer)){
						Log.warn(String.format("Failed to send client input event: clientId = %d, clientIndex = %d", 
								client.getClientId(), 
								currentClientIndex));
					} else {
						client.advanceSendTime();
					}
				}
			}
		}
	}
	
	private void processClientRecv(byte[] incomingBuffer) {
		// create metrics
	}
	
	private void createClientEvent(final Client client, final byte[] outgoingBuffer) {
		long sendTime = System.nanoTime();
		long actionId = client.addSentAction(sendTime);
		_header.setIsValid(true, outgoingBuffer);
		_inputEvent.setBackingArray(outgoingBuffer, _header.getEventOffset());
		try {
			_inputEvent.setClientId(client.getClientId());
			_inputEvent.setClientActionId(actionId);			
		} finally {
			_inputEvent.releaseBackingArray();
		}
	}
	
	private void sendMetricEvent() {
		
	}
	
	private long getWaitTime(long nextSendTime) {
		long remainingTime = nextSendTime - System.nanoTime();
		if (remainingTime < 0)
			remainingTime = 0;
		return remainingTime;
	}
	
}
