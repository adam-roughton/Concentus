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
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferReader;
import com.adamroughton.concentus.disruptor.NonBlockingRingBufferWriter;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SendRecvMessengerReactor;
import com.adamroughton.concentus.messaging.MessengerMutex;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketMessenger;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Util;
import com.google.caliper.Param;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

public class SendRecvSocketReactorBenchmark extends MessagingBenchmarkBase {

	private final int _port;
	private ZMQ.Socket _sendSocket;
	private IncomingEventHeader _recvHeader;
	private OutgoingEventHeader _sendHeader;
	private RingBuffer<byte[]> _recvBuffer;
	private RingBuffer<byte[]> _sendBuffer;
	private byte[] _msg;
	
	@Param int messageCount;
	@Param int messageSize;
	@Param boolean sendAndRecv;
	
	public SendRecvSocketReactorBenchmark(int port) {
		_port = port;
	}
	
	@Override
	protected void setUp(Context context) throws Exception {
		super.setUp(context);
		
		_recvHeader = new IncomingEventHeader(0, 1);
		_sendHeader = new OutgoingEventHeader(0, 1);
		
		_recvBuffer = new RingBuffer<>(
				Util.msgBufferFactory(Util.nextPowerOf2(messageSize + _recvHeader.getEventOffset())), 
				new SingleThreadedClaimStrategy(1), 
				new YieldingWaitStrategy());
		_recvBuffer.setGatingSequences(new Sequence(Long.MAX_VALUE));
		
		_sendBuffer = new RingBuffer<>(
				Util.msgBufferFactory(Util.nextPowerOf2(messageSize + _recvHeader.getEventOffset())), 
				new SingleThreadedClaimStrategy(1), 
				new YieldingWaitStrategy());
		_sendBuffer.setGatingSequences(new Sequence(Long.MAX_VALUE));
		if (sendAndRecv) {
			_sendBuffer.forcePublish(Long.MAX_VALUE);
		}
		
		byte[] entryContent = _sendBuffer.get(0);
		_sendHeader.setSegmentMetaData(entryContent, 0, 0, messageSize);
		_sendHeader.setIsValid(entryContent, true);
		
		_sendSocket = context.socket(ZMQ.DEALER);
		_sendSocket.setLinger(0);
		_sendSocket.setSendTimeOut(1000);
		_sendSocket.connect("tcp://127.0.0.1:" + _port);
		
		_msg = new byte[messageSize];
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
		
		final ZMQ.Socket dealerSocket = context.socket(ZMQ.DEALER);
		dealerSocket.setReceiveTimeOut(1000);
		dealerSocket.setSendTimeOut(1000);
		dealerSocket.setLinger(0);
		dealerSocket.setHWM(1); // drop out-bound messages (DEALER behaviour)
		dealerSocket.bind("tcp://127.0.0.1:" + _port);
		ZmqSocketMessenger messenger = new ZmqSocketMessenger(0, dealerSocket, new DefaultClock());
		
		Mutex<Messenger> mutex = new MessengerMutex<ZmqSocketMessenger>(messenger);
		FatalExceptionCallback exCallback = new FatalExceptionCallback() {
			
			@Override
			public void signalFatalException(Throwable exception) {
				exception.printStackTrace();
				throw new RuntimeException(exception);
			}
		};
		
		OutgoingEventHeader sendHeader = new OutgoingEventHeader(0, 1);
		
		SequenceBarrier barrier = _sendBuffer.newBarrier();
		NonBlockingRingBufferWriter<byte[]> recvWriter = new NonBlockingRingBufferWriter<>(_recvBuffer);
		NonBlockingRingBufferReader<byte[]> sendReader = new NonBlockingRingBufferReader<>(_sendBuffer, barrier);
		
		final SendRecvMessengerReactor reactor = new SendRecvMessengerReactor(mutex, sendHeader, _recvHeader, recvWriter, sendReader, exCallback);
		
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
		while (_recvBuffer.getCursor() < messageCount) {
			_sendSocket.send(_msg, 0, 0);
		}
	}

	public static void main(String[] args) throws Exception {
		while(true) {
			SendRecvSocketReactorBenchmark benchmark = new SendRecvSocketReactorBenchmark(9000);
			benchmark.messageCount = 1000000;
			benchmark.messageSize = 512;
			benchmark.sendAndRecv = true;
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
