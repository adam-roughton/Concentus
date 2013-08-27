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
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketMessenger;
import com.adamroughton.concentus.messaging.zmq.ZmqStandardSocketMessenger;
import com.adamroughton.concentus.util.Util;
import com.google.caliper.Param;

public class PublisherBenchmark extends MessagingBenchmarkBase {

	private final int _port;
	private ZMQ.Socket _recvSocket;
	private ZmqSocketMessenger _messenger;
	private ArrayBackedResizingBuffer _recvBuffer;
	private IncomingEventHeader _header;
	
	@Param int messageCount;
	@Param boolean isBlockingRecv;
	@Param int messageSize;
	
	public PublisherBenchmark(int port) {
		_port = port;
	}
	
	@Override
	protected void setUp(Context context) throws Exception {
		super.setUp(context);
		_recvSocket = context.socket(ZMQ.DEALER);
		_recvSocket.setReceiveTimeOut(1000);
		_recvSocket.bind("tcp://127.0.0.1:" + _port);
		_messenger = new ZmqStandardSocketMessenger(0, "", _recvSocket, new DefaultClock());
	
		_header = new IncomingEventHeader(0, 1);
		_recvBuffer = new ArrayBackedResizingBuffer(Util.nextPowerOf2(messageSize + _header.getEventOffset()));
	}

	@Override
	protected void tearDown() throws Exception {
		if (_recvSocket != null) {
			_recvSocket.close();
		}
		super.tearDown();
	}

	@Override
	protected Runnable createInteractingParty(Context context,
			final AtomicBoolean runFlag) {
		
		ZMQ.Socket socket = context.socket(ZMQ.DEALER);
		socket.setLinger(0);
		socket.setSendTimeOut(1000);
		socket.connect("tcp://127.0.0.1:" + _port);
		final ZmqSocketMessenger messenger = new ZmqStandardSocketMessenger(0, "", socket, new DefaultClock());
		
		OutgoingEventHeader sendHeader = new OutgoingEventHeader(0, 1);
		int msgSize = Util.nextPowerOf2(messageSize + sendHeader.getEventOffset());
		final ArrayBackedResizingBuffer msg = new ArrayBackedResizingBuffer(msgSize);
		sendHeader.setSegmentMetaData(msg, 0, sendHeader.getEventOffset(), msgSize - sendHeader.getEventOffset());
		sendHeader.setIsValid(msg, true);
		final Publisher<ArrayBackedResizingBuffer> publisher = new Publisher<>(sendHeader);
		
		return new Runnable() {
			
			@Override
			public void run() {
				long seq = 0;
				try {
					while (runFlag.get()) {
						try {
							publisher.onEvent(msg, seq++, false, messenger);
						} catch (Exception e){
							throw new RuntimeException(e);
						}
 					}
				} finally {
					messenger.getSocket().close();
				}
			}
		};
	}
	
	public long timePublisher() {
		long recvCount = 0;
		long val = 0;
		while (recvCount < messageCount) {
			if (_messenger.recv(_recvBuffer, _header, isBlockingRecv)) {
				val ^= _recvBuffer.readByte(4);
				recvCount++;
			}
		}
		return val;
	}

	public static void main(String[] args) throws Exception {
		while(true) {
			PublisherBenchmark benchmark = new PublisherBenchmark(9000);
			benchmark.messageCount = 1000000;
			benchmark.isBlockingRecv = false;
			benchmark.messageSize = 32;
			benchmark.setUp();
			
			long startTime = System.nanoTime();
			benchmark.timePublisher();
			long duration = System.nanoTime() - startTime;
			double throughput = ((double) benchmark.messageCount / duration) * TimeUnit.SECONDS.toNanos(1);
			System.out.println(String.format("%f msgs/s", throughput));
			
			benchmark.tearDown();
		}
	}
	
}
