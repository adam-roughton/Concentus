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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.adamroughton.concentus.DrivableClock;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.SingleProducerEventQueue;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;
import com.adamroughton.concentus.messaging.events.ClientConnectEvent;
import com.adamroughton.concentus.messaging.events.ClientHandlerEntry;
import com.adamroughton.concentus.messaging.events.ClientInputEvent;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.events.EventType;
import com.adamroughton.concentus.messaging.events.StateUpdateEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateInfoEvent;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

public class PerfTestClientHandlerProcessor {
	
	public static void main(String[] args) throws Exception {
		System.in.read();
		for (int i = 1000; i <= 100000; i += 1000) {
			doRun(i);
		}
	}
	
	private static EventQueue<byte[]> createQueue() {
		return new SingleProducerEventQueue<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[512];
			}
			
		}, 1024 * 1024,
			new YieldingWaitStrategy());
	}
	
	private static <TEvent extends ByteArrayBackedEvent> void fakeRecv(
			byte[] buffer, 
			IncomingEventHeader header,
			int firstSegData,
			int socketId,
			TEvent event, 
			EventWriter<OutgoingEventHeader, TEvent> writer) throws Exception {
		int cursor = header.getEventOffset();
		header.setSegmentMetaData(buffer, 0, cursor, 4);
		MessageBytesUtil.writeInt(buffer, cursor, firstSegData);
		cursor += 4;
		event.setBackingArray(buffer, cursor);
		try {
			event.writeEventTypeId();
			writer.write(null, event);
			header.setSegmentMetaData(buffer, 1, cursor, event.getEventSize());
			header.setSocketId(buffer, socketId);
			header.setIsValid(buffer, true);
		} finally {
			event.releaseBackingArray();	
		}
	}
	
	private static int getContentOffset(byte[] event, OutgoingEventHeader header) {
		int contentSegIndex = header.getSegmentCount() - 1;
		int contentSegMetaData = header.getSegmentMetaData(event, contentSegIndex);
		return EventHeader.getSegmentOffset(contentSegMetaData);
	}
	
	private static int getEventType(byte[] event, OutgoingEventHeader header) {
		return MessageBytesUtil.readInt(event, getContentOffset(event, header));
	}
	
	private static <TEvent extends ByteArrayBackedEvent> void readContent(byte[] event, 
			OutgoingEventHeader header, TEvent eventHelper, EventReader<IncomingEventHeader, TEvent> reader) {
		int contentOffset = getContentOffset(event, header);
		eventHelper.setBackingArray(event, contentOffset);
		try {
			reader.read(null, eventHelper);
		} finally {
			eventHelper.releaseBackingArray();
		}
	}
	
	private static void doRun(final int clientCount) throws Exception {
		ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setDaemon(true);
				return t;
			}
		});
		
		final IncomingEventHeader recvHeader = new IncomingEventHeader(0, 2);
		final OutgoingEventHeader sendHeader = new OutgoingEventHeader(0, 2);
		final DrivableClock testClock = new DrivableClock();
		
		final EventQueue<byte[]> genericOutQueue = createQueue();
		
		final AtomicLong testVal = new AtomicLong();
		final AtomicLong updatesRecvd = new AtomicLong();
		final AtomicLong connectResRecved = new AtomicLong();
		final AtomicLong unknownEventCount = new AtomicLong();
		final AtomicLong metricsRecv = new AtomicLong();
		final AtomicLong stateInputsProc = new AtomicLong();
		final EventProcessor genericConsumer = genericOutQueue.createEventProcessor(new EventHandler<byte[]>() {

			final ClientUpdateEvent updateEvent = new ClientUpdateEvent();
			final ConnectResponseEvent connectResEvent = new ConnectResponseEvent();
			
			@Override
			public void onEvent(byte[] event, long sequence, boolean endOfBatch)
					throws Exception {
				final long val = testVal.get();
				int eventType = getEventType(event, sendHeader);
				if (eventType == EventType.CONNECT_RES.getId()) {
					readContent(event, sendHeader, connectResEvent, new EventReader<IncomingEventHeader, ConnectResponseEvent>() {

						@Override
						public void read(IncomingEventHeader header,
								ConnectResponseEvent event) {
							testVal.lazySet(val ^ event.getCallbackBits() ^ event.getEventSize() ^ event.getResponseCode());
							connectResRecved.incrementAndGet();
						}
						
					});
				} else if (eventType == EventType.CLIENT_UPDATE.getId()) {
					readContent(event, sendHeader, updateEvent, new EventReader<IncomingEventHeader, ClientUpdateEvent>() {

						@Override
						public void read(IncomingEventHeader header,
								ClientUpdateEvent event) {
							testVal.lazySet(val ^ event.getClientId() ^ event.getEventSize() ^ event.getSimTime() ^ event.getUpdateId() ^ event.getHighestInputActionId());
							updatesRecvd.incrementAndGet();
						}
						
					});
				} else if (eventType == EventType.STATE_INPUT.getId()) {
					stateInputsProc.incrementAndGet();
				} else if (eventType == EventType.CLIENT_HANDLER_METRIC.getId()) {
					metricsRecv.incrementAndGet();
				} else {
					unknownEventCount.incrementAndGet();
					//System.out.println(String.format("Unknown event type %d", eventType));
				}
			}
		});
		
		SendQueue<OutgoingEventHeader> routerSendQueue = new SendQueue<>(sendHeader, genericOutQueue);
		SendQueue<OutgoingEventHeader> pubSendQueue = new SendQueue<>(sendHeader, genericOutQueue);
		SendQueue<OutgoingEventHeader> metricSendQueue = new SendQueue<>(sendHeader, genericOutQueue);
		
		final int routerSocketId = 0;
		final int subSocketId = 1;
		
		final ClientHandlerProcessor clientHandler = new ClientHandlerProcessor(testClock, 0, routerSocketId, subSocketId, routerSendQueue, pubSendQueue, metricSendQueue, recvHeader);
		final byte[] buffer = new byte[512];
		
		// consumes events to prevent test being optimised away
		Future<?> consumerFuture = executor.submit(genericConsumer);
		
		// connect
		Future<?> connectFuture = executor.submit(new Runnable() {

			@Override
			public void run() {
				ClientConnectEvent connEvent = new ClientConnectEvent();
				for (int seq = 0; seq < clientCount; seq++) {
					final int clientId = seq;
					try {
						fakeRecv(buffer, recvHeader, clientId, routerSocketId, connEvent, new EventWriter<OutgoingEventHeader, ClientConnectEvent>() {
	
							@Override
							public void write(OutgoingEventHeader header,
									ClientConnectEvent event) throws Exception {
								event.setCallbackBits(clientId);
							}
							
						});
						clientHandler.onEvent(buffer, seq, true);
						testClock.advance(300, TimeUnit.NANOSECONDS);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}
			
		});
		connectFuture.get();
		
		final AtomicLong updateCount = new AtomicLong();
		Future<Long> perfFuture = executor.submit(new Callable<Long>() {

			@Override
			public Long call() throws Exception {
				final long startTime = System.currentTimeMillis();
				ClientInputEvent inputEvent = new ClientInputEvent();
				StateUpdateEvent updateEvent = new StateUpdateEvent();
				StateUpdateInfoEvent infoEvent = new StateUpdateInfoEvent();
				
				long nextDeadline = 0;
				long nextUpdateId = 0;
				long seq = clientCount;
				for (long i = 0; i < clientCount * 1000; i++) {
					if (i % clientCount == 37) {
						nextDeadline = clientHandler.moveToNextDeadline(0);
					}
					
					final int clientId = (int) i % clientCount;
					final long actionId = i / clientCount;
					if (i % clientCount == 42) {
						updateCount.incrementAndGet();
						
						// send update
						final int updateLength = 250;
						final long updateId = nextUpdateId++;
						
						fakeRecv(buffer, recvHeader, EventType.STATE_UPDATE.getId(), subSocketId, updateEvent, new EventWriter<OutgoingEventHeader, StateUpdateEvent>() {

							@Override
							public void write(OutgoingEventHeader header,
									StateUpdateEvent event) throws Exception {
								event.setUpdateId(updateId);
								event.setSimTime(testClock.currentMillis());
								event.setUsedLength(updateLength);
							}
						});
						clientHandler.onEvent(buffer, seq++, true);
						
						fakeRecv(buffer, recvHeader, EventType.STATE_INFO.getId(), subSocketId, infoEvent, new EventWriter<OutgoingEventHeader, StateUpdateInfoEvent>() {

							@Override
							public void write(OutgoingEventHeader header,
									StateUpdateInfoEvent event)
									throws Exception {
								event.setUpdateId(updateId);
								event.setEntryCount(event.getMaximumEntries());
								long highestActionId = actionId - 4;
								highestActionId = highestActionId < -1? -1 : highestActionId;
								for (int j = 0; j < event.getMaximumEntries(); j++) {
									event.setHandlerEntry(j, new ClientHandlerEntry(j, highestActionId));
								}
							}
						});
						clientHandler.onEvent(buffer, seq++, true);
					} 
					
					// recv input event
					fakeRecv(buffer, recvHeader, clientId, routerSocketId, inputEvent, new EventWriter<OutgoingEventHeader, ClientInputEvent>() {

						@Override
						public void write(OutgoingEventHeader header,
								ClientInputEvent event) throws Exception {
							event.setClientActionId(actionId);
							event.setClientId(clientId);
							event.setUsedLength(250);
						}
						
					});
					clientHandler.onEvent(buffer, seq++, true);
					
					
					if (i % clientCount == 37) {
						testClock.setTime(1000 * (i / clientCount), TimeUnit.MILLISECONDS);
						if (testClock.currentMillis() > nextDeadline) {
							clientHandler.onDeadline();
						}
					}
				}			
				return System.currentTimeMillis() - startTime;
			}
			
		});
		
		long duration = perfFuture.get();
		long eventsProcessed = 1000 * clientCount;
		double throughput = ((double) eventsProcessed) / ((double) duration) * 1000;
		System.out.println(String.format("%d: %f (%d events processed, %d milliseconds, %d/%d updates recvd, %d conn res recvd, %d metrics, %d state inputs, %d unknown events) " +
				"(test val for avoiding optimisation = %d)", 
				clientCount, 
				throughput, 
				eventsProcessed, 
				duration, 
				updatesRecvd.get(),
				updateCount.get() * clientCount,
				connectResRecved.get(),
				metricsRecv.get(),
				stateInputsProc.get(),
				unknownEventCount.get(),
				testVal.get()));
		
		genericConsumer.halt();
		consumerFuture.get();
		executor.shutdownNow();
		executor.awaitTermination(5, TimeUnit.SECONDS);
	}
	
	
}
