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

import java.util.concurrent.TimeUnit;

import uk.co.real_logic.intrinsics.ComponentFactory;
import uk.co.real_logic.intrinsics.StructuredArray;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.SingleProducerEventQueue;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.YieldingWaitStrategy;

public class SimulatedClientProcessorBenchmark {

	private Clock _clock;
	private StructuredArray<Client> _clients;
	private SimulatedClientProcessor _worker;
	private IncomingEventHeader _header;
	private EventQueue<byte[]> _sendQueue;
	private long _testStartSeq;
	
	private byte[] _recvBuffer;
	
	public void setUp() {
		int clientCount = 50000;
		int bufferSize = 512;
		
		_clock = new DefaultClock();
		_clients = StructuredArray.newInstance(Util.nextPowerOf2(clientCount), Client.class, new ComponentFactory<Client>() {

			long nextId = 0;
			
			@Override
			public Client newInstance(Object[] initArgs) {
				return new Client(nextId++, _clock);
			}
			
		});
		_recvBuffer = new byte[bufferSize];
		
		_sendQueue = new SingleProducerEventQueue<>(Util.msgBufferFactory(bufferSize), 
				1,
				new YieldingWaitStrategy());
		_sendQueue.addGatingSequences(new Sequence(Long.MAX_VALUE));
		
		_header = new IncomingEventHeader(0, 2);
		
		SendQueue<OutgoingEventHeader> clientSendQueue = new SendQueue<>(new OutgoingEventHeader(0, 1), _sendQueue);
		SendQueue<OutgoingEventHeader> metricSendQueue = new SendQueue<>(new OutgoingEventHeader(0, 2), _sendQueue);
		
		_worker = new SimulatedClientProcessor(0, _clock, _clients, clientCount, clientSendQueue, metricSendQueue, _header);
		
		MetricEntry nullMetricEntry = new MetricEntry() {
			
			@Override
			public void incrementSentInputCount(int amount) {
			}
			
			@Override
			public void incrementInputToUpdateLateCount(int amount) {
			}
			
			@Override
			public void incrementConnectedClientCount(int amount) {
			}
			
			@Override
			public void addInputToUpdateLatency(long latency) {
			}
		};
		
		ConnectResponseEvent connRes = new ConnectResponseEvent();
		connRes.setBackingArray(_recvBuffer, 0);
		for (int i = 0; i < _clients.getLength(); i++) {
			Client client = _clients.get(i);
			client.setHandlerId(0);
			client.setIsActive(true);
			
			// generate connect request
			client.onActionDeadline(clientSendQueue, nullMetricEntry);
			
			connRes.setClientId(i);
			connRes.setCallbackBits(i);
			connRes.setResponseCode(ConnectResponseEvent.RES_OK);
			
			client.onConnectResponse(connRes, nullMetricEntry);
		}
		connRes.releaseBackingArray();
		
		_testStartSeq = _sendQueue.getCursor();
		_worker.onStart();
	}	
	
	public void timeWorker(int numUpdates, long spacingBetweenUpdates) throws Exception {
		ClientUpdateEvent updateEvent = new ClientUpdateEvent();
		updateEvent.setBackingArray(_recvBuffer, 0);
		
		long seq = 0;
		for (int updateIndex = 0; updateIndex < numUpdates; updateIndex++) {
			long nextDeadline = System.currentTimeMillis() + spacingBetweenUpdates;
			do {
				_worker.moveToNextDeadline(0);
				_worker.onDeadline();
			} while (System.currentTimeMillis() < nextDeadline);
			
			for (Client client : _clients) {
				updateEvent.setClientId(client.getClientId());
				updateEvent.setUpdateId(updateIndex);
				updateEvent.setSimTime(System.currentTimeMillis());
				updateEvent.setHighestInputActionId(0);
				updateEvent.setUsedLength(updateEvent.getMaxUpdateBufferLength());
				_worker.onEvent(_recvBuffer, seq++, true);
			}
		}
		updateEvent.releaseBackingArray();
	}
	
	public long eventSentCount() {
		return _sendQueue.getCursor() - _testStartSeq;
	}
	
	public static void main(String[] args) {
		int updateCount = 1;
		long updateSpacing = 100000;
		
		for (;;) {
			try {
				SimulatedClientProcessorBenchmark benchmark = new SimulatedClientProcessorBenchmark();
				benchmark.setUp();
			
				long startTime = System.nanoTime();
				benchmark.timeWorker(updateCount, updateSpacing);
				long duration = System.nanoTime() - startTime;
				
				double sentThroughput = (double) benchmark.eventSentCount() / TimeUnit.NANOSECONDS.toSeconds(duration);
				double recvThroughput = (double) updateCount / TimeUnit.NANOSECONDS.toSeconds(duration);
				
				System.out.println(String.format("[%f msgsSent; %f msgsProc]/s", sentThroughput, recvThroughput));
			} catch (Exception e) {
				e.printStackTrace();
			}
		
		}
	}

}
