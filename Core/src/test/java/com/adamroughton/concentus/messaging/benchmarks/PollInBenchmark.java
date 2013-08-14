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

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.zmq.HackSocketPollInSet;
import com.adamroughton.concentus.messaging.zmq.SocketPollInSet;
import com.adamroughton.concentus.messaging.zmq.SocketPollSet;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketMessenger;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketSetMessenger;
import com.adamroughton.concentus.messaging.zmq.ZmqStandardSocketMessenger;
import com.google.caliper.Param;

public class PollInBenchmark extends MessagingBenchmarkBase {

	private final int _startPort;
	private ZMQ.Socket[] _recvSockets;
	private ZmqSocketSetMessenger _pollSetMessenger;
	private byte[] _recvBuffer;
	private IncomingEventHeader _header;
	private SocketPollSet _socketPollSet;
	
	@Param int socketCount;
	@Param int messageCount;
	@Param boolean isBlockingRecv;
	@Param int messageSize;
	@Param boolean useAlternativePollIn;
	
	public PollInBenchmark(int startPort) {
		_startPort = startPort;
	}
	
	@Override
	protected void setUp(Context context) throws Exception {
		super.setUp(context);
		_recvSockets = new ZMQ.Socket[socketCount];
		ZmqSocketMessenger[] messengers = new ZmqStandardSocketMessenger[socketCount];
		Clock clock = new DefaultClock();
		for (int i = 0; i < socketCount; i++) {
			ZMQ.Socket socket = context.socket(ZMQ.DEALER);
			socket.setReceiveTimeOut(1000);
			socket.bind("tcp://127.0.0.1:" + (_startPort + i));
			_recvSockets[i] = socket;
			messengers[i] = new ZmqStandardSocketMessenger(i, "", socket, clock);
		}
		if (useAlternativePollIn) {
			_socketPollSet = new HackSocketPollInSet(messengers);
		} else {
			_socketPollSet = new SocketPollInSet(context, messengers);			
		}
		_pollSetMessenger = new ZmqSocketSetMessenger(_socketPollSet);
		_recvBuffer = new byte[512];
		_header = new IncomingEventHeader(0, 1);
	}

	@Override
	protected void tearDown() throws Exception {
		if (_recvSockets != null) {
			for (ZMQ.Socket socket : _recvSockets) {
				socket.close();
			}
		}
		super.tearDown();
	}

	@Override
	protected Runnable createInteractingParty(Context context,
			final AtomicBoolean runFlag) {
		final ZMQ.Socket[] sockets = new ZMQ.Socket[socketCount];
		for (int i = 0; i < socketCount; i++) {
			ZMQ.Socket socket = context.socket(ZMQ.DEALER);
			socket.setLinger(0);
			socket.setSendTimeOut(1000);
			socket.connect("tcp://127.0.0.1:" + (_startPort + i));
			sockets[i] = socket;
		}
		final byte[] msg = new byte[messageSize];
		
		return new Runnable() {
			
			@Override
			public void run() {
				long seq = 0;
				try {
					while (runFlag.get()) {
						ZMQ.Socket socket = sockets[(int) (seq++ % socketCount)];
						socket.send(msg, 0);
					}
				} finally {
					for (ZMQ.Socket socket : sockets) {
						socket.close();
					}
				}
			}
		};
	}
	
	public long timePollIn() {
		long recvCount = 0;
		long val = 0;
		while (recvCount < messageCount) {
			if (_pollSetMessenger.recv(_recvBuffer, _header, isBlockingRecv)) {
				val ^= _recvBuffer[4];
				recvCount++;
			}
		}
		return val;
	}
	
	public long timeStandardIn() {
		long seq = 0;
		long recvCount = 0;
		long val = 0;
		while (recvCount < messageCount) {
			if (_socketPollSet.getMessenger((int) (seq++ % socketCount)).recv(_recvBuffer, _header, isBlockingRecv)) {
				val ^= _recvBuffer[4];
				recvCount++;
			}
		}
		return val;
	}

	public static void main(String[] args) throws Exception {
		while(true) {
			PollInBenchmark benchmark = new PollInBenchmark(9000);
			benchmark.socketCount = 1;
			benchmark.messageCount = 1000000;
			benchmark.isBlockingRecv = false;
			benchmark.messageSize = 32;
			benchmark.useAlternativePollIn = true;
			benchmark.setUp();
			
			long startTime = System.nanoTime();
			benchmark.timePollIn();
			//benchmark.timeStandardIn();
			long duration = System.nanoTime() - startTime;
			double throughput = ((double) benchmark.messageCount / duration) * TimeUnit.SECONDS.toNanos(1);
			System.out.println(String.format("%f msgs/s", throughput));
			
			benchmark.tearDown();
		}
	}
	
}
