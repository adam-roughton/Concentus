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
package com.adamroughton.concentus.messaging.benchmarks;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ArrayBackedResizingBufferFactory;
import com.adamroughton.concentus.data.BytesUtil;
import com.adamroughton.concentus.disruptor.CollocatedBufferEventFactory;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueImpl;
import com.adamroughton.concentus.disruptor.EventQueuePublisher;
import com.adamroughton.concentus.disruptor.EventQueueStrategyBase;
import com.adamroughton.concentus.disruptor.SingleProducerEventQueuePublisher;
import com.adamroughton.concentus.disruptor.SingleProducerQueueStrategy;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SendRecvMessengerReactor;
import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.TrackingMessengerDecorator;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketMessenger;
import com.adamroughton.concentus.messaging.zmq.ZmqStandardSocketMessenger;
import com.adamroughton.concentus.metric.LogMetricContext;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Util;
import com.google.caliper.Param;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.YieldingWaitStrategy;

public class SendRecvSocketReactorBenchmark extends MessagingBenchmarkBase {

	private final int _port;
	private ZMQ.Socket _dealerSocket;
	private IncomingEventHeader _routerRecvHeader;
	private OutgoingEventHeader _routerSendHeader;
	private EventQueue<ArrayBackedResizingBuffer> _recvQueue;
	private EventQueue<ArrayBackedResizingBuffer> _sendQueue;
	private byte[] _msg;
	private static final byte[] DEALER_SOCKET_ID = Util.intToBytes(15);
	static {
		// ensure first bit is raised for Identity (ZMQ reserves IDs starting with binary 0)
		BytesUtil.writeFlagToByte(DEALER_SOCKET_ID, 0, 0, true);
	}
	
	@Param int messageCount;
	@Param int messageSize;
	@Param boolean sendAndRecv;
	
	public SendRecvSocketReactorBenchmark(int port) {
		_port = port;
	}
	
	@Override
	protected void setUp(Context context) throws Exception {
		super.setUp(context);
		
		_routerRecvHeader = new IncomingEventHeader(0, 2);
		_routerSendHeader = new OutgoingEventHeader(0, 2);
		
		Sequence infiniteGate = new Sequence();
		LogMetricContext metricContext = new LogMetricContext(1000, 10000, new DefaultClock());
		metricContext.start(0);
		
		CollocatedBufferEventFactory<ArrayBackedResizingBuffer> recvBufferFactory = new CollocatedBufferEventFactory<>(1, 
				new ArrayBackedResizingBufferFactory(), 
				Util.nextPowerOf2(messageSize + _routerRecvHeader.getEventOffset()));
		_recvQueue = new EventQueueImpl<>(new SingleProducerQueueStrategy<>("", 
				recvBufferFactory, 
				recvBufferFactory.getCount(), 
				new YieldingWaitStrategy()));
		_recvQueue.addGatingSequences(infiniteGate);
		
		CollocatedBufferEventFactory<ArrayBackedResizingBuffer> sendBufferFactory = new CollocatedBufferEventFactory<>(1, 
				new ArrayBackedResizingBufferFactory(), 
				Util.nextPowerOf2(messageSize + _routerSendHeader.getEventOffset() + DEALER_SOCKET_ID.length));
		final RingBuffer<ArrayBackedResizingBuffer> sendBuffer = RingBuffer.createSingleProducer(
				sendBufferFactory, 
				sendBufferFactory.getCount(), 
				new YieldingWaitStrategy());
		
		_sendQueue = new EventQueueImpl<>(new EventQueueStrategyBase<ArrayBackedResizingBuffer>("", sendBuffer) {

			@Override
			public EventQueuePublisher<ArrayBackedResizingBuffer> newQueuePublisher(
					String name,
					boolean isBlocking) {
				return new SingleProducerEventQueuePublisher<>(name, sendBuffer, isBlocking);
			}
			
		});
		
		_sendQueue.addGatingSequences(infiniteGate);
		infiniteGate.set(Long.MAX_VALUE);
		if (sendAndRecv) {
			sendBuffer.publish(Long.MAX_VALUE);
		}
		
		ArrayBackedResizingBuffer entryContent = sendBuffer.get(0);
		int cursor = _routerSendHeader.getEventOffset();
		entryContent.copyFrom(DEALER_SOCKET_ID, 0, cursor, DEALER_SOCKET_ID.length);
		_routerSendHeader.setSegmentMetaData(entryContent, 0, cursor, DEALER_SOCKET_ID.length);
		cursor += DEALER_SOCKET_ID.length;
		_routerSendHeader.setSegmentMetaData(entryContent, 1, cursor, messageSize);
		_routerSendHeader.setIsValid(entryContent, true);
		
		_dealerSocket = context.socket(ZMQ.DEALER);
		_dealerSocket.setLinger(0);
		_dealerSocket.setSendTimeOut(1000);
		_dealerSocket.setIdentity(DEALER_SOCKET_ID);
		_dealerSocket.setHWM(1000);
		_dealerSocket.connect("tcp://127.0.0.1:" + _port);
		
		_msg = new byte[messageSize];
	}

	@Override
	protected void tearDown() throws Exception {
		if (_dealerSocket != null) {
			_dealerSocket.close();
		}
		super.tearDown();
	}

	@Override
	protected Runnable createInteractingParty(Context context,
			final AtomicBoolean runFlag) {
		
		final ZMQ.Socket dealerSocket = context.socket(ZMQ.ROUTER);
		dealerSocket.setReceiveTimeOut(1000);
		dealerSocket.setSendTimeOut(1000);
		dealerSocket.setLinger(0);
		dealerSocket.setHWM(1000); // drop out-bound messages (DEALER behaviour)
		dealerSocket.bind("tcp://127.0.0.1:" + _port);
		
		LogMetricContext metricContext = new LogMetricContext(1000, 10000, new DefaultClock());
		metricContext.start(1);
		ZmqSocketMessenger messenger = new ZmqStandardSocketMessenger(0, "", dealerSocket);
		TrackingMessengerDecorator<ArrayBackedResizingBuffer> trackingMessenger = new TrackingMessengerDecorator<>(metricContext, messenger, new DefaultClock());
		
		Mutex<Messenger<ArrayBackedResizingBuffer>> mutex = new MessengerMutex<>(trackingMessenger);
		FatalExceptionCallback exCallback = new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
				exception.printStackTrace();
				throw new RuntimeException(exception);
			}
		};
		
		OutgoingEventHeader sendHeader = new OutgoingEventHeader(0, 1);
		
		final SendRecvMessengerReactor<ArrayBackedResizingBuffer> reactor = new SendRecvMessengerReactor<>("", mutex, sendHeader, _routerRecvHeader, _recvQueue, _sendQueue, exCallback);
		
		return new Runnable() {

			@Override
			public void run() {
				try {
					reactor.run();
				} finally {
					dealerSocket.close();
				}
			}
			
		};		
	}
	
	public void timeListener() {
		while (_recvQueue.getCursor() < messageCount) {
			_dealerSocket.send(_msg, 0, 0);
		}
	}

	public static void main(String[] args) throws Exception {
		while(true) {
			SendRecvSocketReactorBenchmark benchmark = new SendRecvSocketReactorBenchmark(9000);
			benchmark.messageCount = 1000000;
			benchmark.messageSize = 512;
			benchmark.sendAndRecv = false;
			benchmark.setUp();
			
			long startTime = System.nanoTime();
			benchmark.timeListener();
			long duration = System.nanoTime() - startTime;
			double throughput = ((double) benchmark.messageCount / duration) * TimeUnit.SECONDS.toNanos(1);
			System.out.println(String.format("%f msgs/s", throughput));
			
			benchmark.tearDown();
		}
	}
	
}
