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
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueImpl;
import com.adamroughton.concentus.disruptor.SingleProducerQueueStrategy;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketMessenger;
import com.adamroughton.concentus.util.Util;
import com.google.caliper.Param;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.YieldingWaitStrategy;

public class EventListenerBenchmark extends MessagingBenchmarkBase {

	private final int _port;
	private ZMQ.Socket _sendSocket;
	private IncomingEventHeader _header;
	private EventQueue<byte[]> _recvQueue;
	private byte[] _sendBuffer;
	
	@Param int messageCount;
	@Param boolean isBlockingRecv;
	@Param int messageSize;
	
	public EventListenerBenchmark(int port) {
		_port = port;
	}
	
	@Override
	protected void setUp(Context context) throws Exception {
		super.setUp(context);
		
		_header = new IncomingEventHeader(0, 1);
		
		_recvQueue = new EventQueueImpl<>(new SingleProducerQueueStrategy<>("", 
				Util.msgBufferFactory(Util.nextPowerOf2(messageSize + _header.getEventOffset())), 
				1, 
				new YieldingWaitStrategy()));
		_recvQueue.addGatingSequences(new Sequence(Long.MAX_VALUE));
		
		_sendSocket = context.socket(ZMQ.DEALER);
		_sendSocket.setLinger(0);
		_sendSocket.setSendTimeOut(1000);
		_sendSocket.connect("tcp://127.0.0.1:" + _port);
		
		_sendBuffer = new byte[messageSize];
	}

	@Override
	protected void tearDown() throws Exception {
		if (_sendSocket != null) {
			_sendSocket.close();
		}
		super.tearDown();
	}

	@Override
	protected Runnable createInteractingParty(Context context,
			final AtomicBoolean runFlag) {
		
		final ZMQ.Socket recvSocket = context.socket(ZMQ.DEALER);
		recvSocket.setReceiveTimeOut(1000);
		recvSocket.bind("tcp://127.0.0.1:" + _port);
		ZmqSocketMessenger messenger = new ZmqSocketMessenger(0, "", recvSocket, new DefaultClock());
		
		MessengerMutex<ZmqSocketMessenger> mutex = new MessengerMutex<ZmqSocketMessenger>(messenger);
		FatalExceptionCallback exCallback = new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
				exception.printStackTrace();
				throw new RuntimeException(exception);
			}
		};
		
		final EventListener listener = new EventListener("", _header, mutex, _recvQueue, exCallback);
		
		return new Runnable() {

			@Override
			public void run() {
				try {
					listener.run();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					recvSocket.close();
				}
			}
			
		};		
	}
	
	public void timeListener() {
		while (_recvQueue.getCursor() < messageCount) {
			_sendSocket.send(_sendBuffer, 0, 0);
		}
	}

	public static void main(String[] args) throws Exception {
		while(true) {
			EventListenerBenchmark benchmark = new EventListenerBenchmark(9000);
			benchmark.messageCount = 1000000;
			benchmark.isBlockingRecv = false;
			benchmark.messageSize = 32;
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
