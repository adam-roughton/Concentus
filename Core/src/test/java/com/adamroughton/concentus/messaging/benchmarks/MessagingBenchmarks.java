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

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketMessenger;
import com.adamroughton.concentus.util.Util;
import com.google.caliper.*;
import com.google.caliper.api.Macrobenchmark;
import com.google.caliper.runner.CaliperMain;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class MessagingBenchmarks {

	public static class SendBenchmark extends MessagingBenchmarkBase {

		@Param private int msgCount;
		@Param private int msgSegmentCount;
		@Param private boolean isBlocking;
		@Param private int bufferSize;
		
		private OutgoingEventHeader _header;
		private byte[] _sendBuffer;
		private ZMQ.Socket _testSocket;
		private ZmqSocketMessenger _messenger;

		@Override
		protected void setUp(ZMQ.Context context) throws Exception {			
			super.setUp(context);
			_header = new OutgoingEventHeader(0, msgSegmentCount);
			_sendBuffer = new byte[bufferSize];
			// divide the buffer into equal (or close to equal) segment portions
			int segmentSize = (_sendBuffer.length - _header.getEventOffset()) / msgSegmentCount;
			int cursor = _header.getEventOffset();
			for (int i = 0; i < msgSegmentCount; i++) {
				_header.setSegmentMetaData(_sendBuffer, i, cursor, segmentSize);
				cursor += segmentSize;
			}
			// set the valid flag
			_header.setIsValid(_sendBuffer, true);
			
			_testSocket = context.socket(ZMQ.DEALER);
			_testSocket.setLinger(0);
			_testSocket.connect("tcp://127.0.0.1:9080");
			_messenger = new ZmqSocketMessenger(0, "", _testSocket, new DefaultClock());
		}
		
		@Override
		protected Runnable createInteractingParty(final ZMQ.Context context,
				final AtomicBoolean runFlag) {
			final ZMQ.Socket socket = context.socket(ZMQ.DEALER);
			socket.setLinger(0);
			socket.bind("tcp://127.0.0.1:9080");
			return new Runnable() {

				@Override
				public void run() {
					try {
						byte[] recvBuffer = new byte[bufferSize];
						while (runFlag.get()) {
							socket.recv(recvBuffer, 0, bufferSize, ZMQ.NOBLOCK);
						}
					} finally {
						socket.close();
					}
				}
			};
		}

		@Override
		protected void tearDown() throws Exception {
			if (_testSocket != null) {
				_testSocket.close();
			}
			super.tearDown();
		}

		@Macrobenchmark
		public void timeSend() {
			for (int i = 0; i < msgCount; i++) {
				_messenger.send(_sendBuffer, _header, isBlocking);
			}
		}
		
		@Macrobenchmark
		public void timeDirectSend() {
			for (int i = 0; i < msgCount; i++) {
				_testSocket.send(_sendBuffer, 0);
			}
		}
		
		public static void main(String[] args) throws Exception {
			for (;;) {
				SendBenchmark sendBench = new SendBenchmark();
				sendBench.msgCount = 10000000;
				sendBench.bufferSize = 512;
				sendBench.msgSegmentCount = 4;
				sendBench.isBlocking = false;
				sendBench.setUp();
				
				long startTime = System.nanoTime();
				//sendBench.timeDirectSend();
				sendBench.timeSend();
				long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
				
				System.out.println(String.format("%f msg/s", (double) sendBench.msgCount / (double) duration));
				
				sendBench.tearDown();
			}
			
			//CaliperMain.main(SendBenchmark.class, args);
		}
		
	}
	
	public static class SendRecvBenchmark extends MessagingBenchmarkBase {

		@Param int msgCount;
		@Param int msgSegmentCount;
		@Param boolean isBlocking;
		@Param int msgSize;
		@Param boolean useInProc;
		@Param boolean recvDirect;
		
		private OutgoingEventHeader _header;
		private byte[] _sendBuffer;
		private ByteBuffer _zeroCopyBuffer;
		private int _segmentSize;
		private ZMQ.Socket _testSocket;
		private ZmqSocketMessenger _messenger;
		
		private AtomicBoolean _allRecv = new AtomicBoolean();

		@Override
		protected void setUp(ZMQ.Context context) throws Exception {		
			super.setUp(context);
			_header = new OutgoingEventHeader(0, msgSegmentCount);
			_sendBuffer = new byte[Util.nextPowerOf2(msgSize + _header.getEventOffset())];
			_zeroCopyBuffer = ByteBuffer.allocateDirect(Util.nextPowerOf2(msgSize + _header.getEventOffset()));
			
			// divide the buffer into equal (or close to equal) segment portions
			_segmentSize = msgSize / msgSegmentCount;
			int cursor = _header.getEventOffset();
			for (int i = 0; i < msgSegmentCount; i++) {
				_header.setSegmentMetaData(_sendBuffer, i, cursor, _segmentSize);
				cursor += _segmentSize;
			}
			_header.setTargetSocketId(_sendBuffer, 0);
			// set the valid flag
			_header.setIsValid(_sendBuffer, true);
			
			_testSocket = context.socket(ZMQ.DEALER);
			_testSocket.setSendTimeOut(5000);
			_testSocket.setLinger(0);
			
			if (useInProc) {
				_testSocket.connect("inproc://recvTest");				
			} else {
				_testSocket.connect("tcp://127.0.0.1:9080");				
			}
			_messenger = new ZmqSocketMessenger(0, "", _testSocket, new DefaultClock());
		}
		
		@Override
		protected Runnable createInteractingParty(final ZMQ.Context context,
				final AtomicBoolean runFlag) {
			_allRecv.set(false);
			
			final ZMQ.Socket socket = context.socket(ZMQ.DEALER);
			socket.setReceiveTimeOut(5000);
			if (useInProc) {
				socket.bind("inproc://recvTest");				
			} else {
				socket.bind("tcp://127.0.0.1:9080");				
			}
			
			if (!recvDirect) {
				final ZmqSocketMessenger messenger = new ZmqSocketMessenger(0, "", socket, new DefaultClock());
				final IncomingEventHeader header = new IncomingEventHeader(0, msgSegmentCount);
				
				return new Runnable() {
	
					@Override
					public void run() {
						long recvCount = 0;
						try {
							byte[] recvBuffer = new byte[Util.nextPowerOf2(msgSize + header.getEventOffset())];
							while (runFlag.get() && recvCount < msgCount) {
								if (messenger.recv(recvBuffer, header, isBlocking)){
									recvCount++;
								}
								
							}
							_allRecv.set(true);
							System.out.println("Recv finished");
						} finally {
							socket.close();
						}
					}
				};
			} else {
				return new Runnable() {
					
					@Override
					public void run() {
						long recvCount = 0;
						//int flag = isBlocking? 0 : ZMQ.NOBLOCK;
						int flag = 0;
						try {
							byte[] recvBuffer = new byte[msgSize];
							ByteBuffer zeroCopyRecvBuffer = ByteBuffer.allocateDirect(msgSize);
							int len = -1;
							while (runFlag.get() && recvCount < msgCount) {
								zeroCopyRecvBuffer.rewind();
								do {
									len = doRecvZeroCopy(socket, zeroCopyRecvBuffer, msgSize, flag);
									//len = socket.recv(recvBuffer, 0, msgSize, flag);
								} while (len == -1 || socket.hasReceiveMore());
								recvCount++;
							}
							_allRecv.set(true);
							System.out.println("Recv finished");
						} finally {
							socket.close();
						}
					}
				};
			}
		}

		@Override
		protected void tearDown() throws Exception {
			if (_testSocket != null) {
				_testSocket.close();
			}
			super.tearDown();
		}

		@Macrobenchmark
		public void timeMessagingSend() {
			while(!_allRecv.get()) {
				if (_messenger.send(_sendBuffer, _header, isBlocking)){
					_header.setIsPartiallySent(_sendBuffer, false);
				}
			}
		}

		@Macrobenchmark
		public void timeJzmqSend() {
			long seq = 0;
			int flag = isBlocking? 0 : ZMQ.NOBLOCK;
			boolean isSocketReady = false;
			while(!_allRecv.get()) {
				if (!isSocketReady) {
					isSocketReady = true;// (_testSocket.getEvents() & ZMQ.Poller.POLLOUT) == ZMQ.Poller.POLLOUT;
				} else {
					boolean successful;
					if (seq % msgSegmentCount < msgSegmentCount - 1) {
						successful = _testSocket.send(_sendBuffer, 0, _segmentSize, ZMQ.SNDMORE | flag);
					} else {
						successful = _testSocket.send(_sendBuffer, 0, _segmentSize, flag);
					}	
					if (successful) {
						seq++;
						isSocketReady = false;
					}
				}
			}
		}
		
		@Macrobenchmark
		public void timeJzmqZeroCopySend() {
			long seq = 0;
			int flag = isBlocking? 0 : ZMQ.NOBLOCK;
			while(!_allRecv.get()) {
				boolean successful;
				if (seq % msgSegmentCount < msgSegmentCount - 1) {
					successful = doSendZeroCopy(_testSocket, _zeroCopyBuffer, _segmentSize, ZMQ.SNDMORE | flag);
				} else {
					successful = doSendZeroCopy(_testSocket, _zeroCopyBuffer, _segmentSize, flag);
				}				
				if (successful) {
					seq++;
				}
			}
		}
		
		private static boolean doSendZeroCopy(ZMQ.Socket socket, ByteBuffer buffer, int size, int flags) {
			try {
				return socket.sendZeroCopy(buffer, size, flags);
			} catch (ZMQException eZmq) {
				if (eZmq.getErrorCode() != 35) {
					throw eZmq;
				} else {
					return false;
				}
			}
		}
		
		private static int doRecvZeroCopy(ZMQ.Socket socket, ByteBuffer buffer, int size, int flags) {
			try {
				return socket.recvZeroCopy(buffer, size, flags);
			} catch (ZMQException eZmq) {
				if (eZmq.getErrorCode() != 35) {
					throw eZmq;
				} else {
					return -1;
				}
			}
		}
		
		public static void main(String[] args) throws Exception {
			for (;;) {
				SendRecvBenchmark recvBench = new SendRecvBenchmark();
				recvBench.msgCount = 10000000;
				recvBench.msgSize = 32;
				recvBench.msgSegmentCount = 1;
				recvBench.isBlocking = true;
				recvBench.useInProc = false;
				recvBench.recvDirect = false;
				recvBench.setUp();
				
				long startTime = System.nanoTime();
				//recvBench.timeJzmqZeroCopySend();
				//recvBench.timeJzmqSend();
				recvBench.timeMessagingSend();
				long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
				
				System.out.println(String.format("%f msg/s", (double) recvBench.msgCount / (double) duration));
				
				recvBench.tearDown();
			}
			
			//CaliperMain.main(SendBenchmark.class, args);
		}
		
	}
	
	public static class RouterSocketBenchmark extends MessagingBenchmarkBase {

		@Param private int socketCount;
		@Param private int msgCount;
		@Param private boolean isBlocking;
		@Param private int msgSize;
		@Param private boolean useInProc;
		@Param private boolean recvDirect;
		
		private OutgoingEventHeader _header;
		private byte[] _sendBuffer;
		private ZMQ.Socket[] _testSockets;
		private ZmqSocketMessenger[] _messengers;
		
		private AtomicBoolean _allRecv = new AtomicBoolean();

		@Override
		protected void setUp(ZMQ.Context context) throws Exception {		
			super.setUp(context);
			_header = new OutgoingEventHeader(0, 2);
			_sendBuffer = new byte[Util.nextPowerOf2(msgSize + _header.getEventOffset())];
			_header.setSegmentMetaData(_sendBuffer, 0, _header.getEventOffset(), msgSize);
			_header.setIsValid(_sendBuffer, true);
			
			_testSockets = new ZMQ.Socket[socketCount];
			_messengers = new ZmqSocketMessenger[socketCount];
			for (int i = 0; i < socketCount; i++) {
				_testSockets[i] = context.socket(ZMQ.DEALER);
				_testSockets[i].setSendTimeOut(5000);
				_testSockets[i].setLinger(0);
				
				if (useInProc) {
					_testSockets[i].connect("inproc://recvTest");				
				} else {
					_testSockets[i].connect("tcp://127.0.0.1:9080");				
				}
				Clock clock = new DefaultClock();
				_messengers[i] = new ZmqSocketMessenger(i, "", _testSockets[i], clock);
			}
		}
		
		@Override
		protected Runnable createInteractingParty(final ZMQ.Context context,
				final AtomicBoolean runFlag) {
			_allRecv.set(false);
			
			final ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
			socket.setLinger(0);
			socket.setReceiveTimeOut(5000);
			if (useInProc) {
				socket.bind("inproc://recvTest");				
			} else {
				socket.bind("tcp://127.0.0.1:9080");				
			}
			
			if (!recvDirect) {
				final ZmqSocketMessenger messenger = new ZmqSocketMessenger(0, "", socket, new DefaultClock());
				final IncomingEventHeader header = new IncomingEventHeader(0, 2);
				
				return new Runnable() {
	
					@Override
					public void run() {
						long recvCount = 0;
						try {
							byte[] recvBuffer = new byte[Util.nextPowerOf2(msgSize + header.getEventOffset())];
							while (runFlag.get() && recvCount < msgCount) {
								if (messenger.recv(recvBuffer, header, isBlocking)){
									recvCount++;
								}
							}
							_allRecv.set(true);
							System.out.println("Recv finished");
						} finally {
							socket.close();
						}
					}
				};
			} else {
				return new Runnable() {
					
					@Override
					public void run() {
						long recvCount = 0;
						int flag = isBlocking? 0 : ZMQ.NOBLOCK;
						try {
							byte[] recvBuffer = new byte[msgSize];
							int len = -1;
							while (runFlag.get() && recvCount < msgCount) {
								do {
									len = socket.recv(recvBuffer, 0, msgSize, flag);
								} while (len == -1 || socket.hasReceiveMore());
								recvCount++;
							}
							_allRecv.set(true);
							System.out.println("Recv finished");
						} finally {
							socket.close();
						}
					}
				};
			}
		}

		@Override
		protected void tearDown() throws Exception {
			if (_testSockets != null) {
				for (int i = 0; i < _testSockets.length; i++) {
					_testSockets[i].close();
				}
			}
			super.tearDown();
		}

		@Macrobenchmark
		public void timeMessagingSend() {
			long seq = 0;
			while(!_allRecv.get()) {
				ZmqSocketMessenger messenger = _messengers[(int) seq++ % _messengers.length];
				if (messenger.send(_sendBuffer, _header, isBlocking)){
					_header.setIsPartiallySent(_sendBuffer, false);
				}
			}
		}
		
		public static void main(String[] args) throws Exception {
			for (;;) {
				RouterSocketBenchmark routerSocketBench = new RouterSocketBenchmark();
				routerSocketBench.msgCount = 10000000;
				routerSocketBench.msgSize = 32;
				routerSocketBench.isBlocking = true;
				routerSocketBench.useInProc = false;
				routerSocketBench.recvDirect = false;
				routerSocketBench.socketCount = 10;
				routerSocketBench.setUp();
				
				long startTime = System.nanoTime();
				//recvBench.timeJzmqZeroCopySend();
				//recvBench.timeJzmqSend();
				routerSocketBench.timeMessagingSend();
				long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
				
				System.out.println(String.format("%f msg/s", (double) routerSocketBench.msgCount / (double) duration));
				
				routerSocketBench.tearDown();
			}
			
			//CaliperMain.main(SendBenchmark.class, args);
		}
		
	}
	
	public static class SendRecvSocketReactorBenchmark extends MessagingBenchmarkBase {

		@Param private int socketCount;
		@Param private int msgCount;
		@Param private boolean isBlocking;
		@Param private int msgSize;
		@Param private boolean useInProc;
		@Param private boolean recvDirect;
		
		private OutgoingEventHeader _header;
		private byte[] _sendBuffer;
		private ZMQ.Socket[] _testSockets;
		private ZmqSocketMessenger[] _messengers;
		
		private AtomicBoolean _allRecv = new AtomicBoolean();

		@Override
		protected void setUp(ZMQ.Context context) throws Exception {		
			super.setUp(context);
			_header = new OutgoingEventHeader(0, 2);
			_sendBuffer = new byte[Util.nextPowerOf2(msgSize + _header.getEventOffset())];
			_header.setSegmentMetaData(_sendBuffer, 0, _header.getEventOffset(), msgSize);
			_header.setIsValid(_sendBuffer, true);
			
			_testSockets = new ZMQ.Socket[socketCount];
			_messengers = new ZmqSocketMessenger[socketCount];
			for (int i = 0; i < socketCount; i++) {
				_testSockets[i] = context.socket(ZMQ.DEALER);
				_testSockets[i].setSendTimeOut(5000);
				_testSockets[i].setLinger(0);
				
				if (useInProc) {
					_testSockets[i].connect("inproc://recvTest");				
				} else {
					_testSockets[i].connect("tcp://127.0.0.1:9080");				
				}
				
				Clock clock = new DefaultClock();
				_messengers[i] = new ZmqSocketMessenger(i, "", _testSockets[i], clock);
			}
		}
		
		@Override
		protected Runnable createInteractingParty(final ZMQ.Context context,
				final AtomicBoolean runFlag) {
			_allRecv.set(false);
			
			final ZMQ.Socket socket = context.socket(ZMQ.ROUTER);
			socket.setLinger(0);
			socket.setReceiveTimeOut(5000);
			if (useInProc) {
				socket.bind("inproc://recvTest");				
			} else {
				socket.bind("tcp://127.0.0.1:9080");				
			}
			
			if (!recvDirect) {
				final ZmqSocketMessenger messenger = new ZmqSocketMessenger(0, "", socket, new DefaultClock());
				final IncomingEventHeader header = new IncomingEventHeader(0, 2);
				
				return new Runnable() {
	
					@Override
					public void run() {
						long recvCount = 0;
						try {
							byte[] recvBuffer = new byte[Util.nextPowerOf2(msgSize + header.getEventOffset())];
							while (runFlag.get() && recvCount < msgCount) {
								if (messenger.recv(recvBuffer, header, isBlocking)){
									recvCount++;
								}
							}
							_allRecv.set(true);
							System.out.println("Recv finished");
						} finally {
							socket.close();
						}
					}
				};
			} else {
				return new Runnable() {
					
					@Override
					public void run() {
						long recvCount = 0;
						int flag = isBlocking? 0 : ZMQ.NOBLOCK;
						try {
							byte[] recvBuffer = new byte[msgSize];
							int len = -1;
							while (runFlag.get() && recvCount < msgCount) {
								do {
									len = socket.recv(recvBuffer, 0, msgSize, flag);
								} while (len == -1 || socket.hasReceiveMore());
								recvCount++;
							}
							_allRecv.set(true);
							System.out.println("Recv finished");
						} finally {
							socket.close();
						}
					}
				};
			}
		}

		@Override
		protected void tearDown() throws Exception {
			if (_testSockets != null) {
				for (int i = 0; i < _testSockets.length; i++) {
					_testSockets[i].close();
				}
			}
			super.tearDown();
		}

		@Macrobenchmark
		public void timeMessagingSend() {
			long seq = 0;
			while(!_allRecv.get()) {
				ZmqSocketMessenger messenger = _messengers[(int) seq++ % _messengers.length];
				if (messenger.send(_sendBuffer, _header, isBlocking)){
					_header.setIsPartiallySent(_sendBuffer, false);
				}
			}
		}
		
		public static void main(String[] args) throws Exception {
			for (;;) {
				RouterSocketBenchmark routerSocketBench = new RouterSocketBenchmark();
				routerSocketBench.msgCount = 10000000;
				routerSocketBench.msgSize = 32;
				routerSocketBench.isBlocking = true;
				routerSocketBench.useInProc = false;
				routerSocketBench.recvDirect = false;
				routerSocketBench.socketCount = 10;
				routerSocketBench.setUp();
				
				long startTime = System.nanoTime();
				//recvBench.timeJzmqZeroCopySend();
				//recvBench.timeJzmqSend();
				routerSocketBench.timeMessagingSend();
				long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
				
				System.out.println(String.format("%f msg/s", (double) routerSocketBench.msgCount / (double) duration));
				
				routerSocketBench.tearDown();
			}
			
			//CaliperMain.main(SendBenchmark.class, args);
		}
		
	}
	
	public static class BenchmarkGetEvents extends Benchmark {
		
		private ZMQ.Context context;
		private ZMQ.Socket socket;
		
		@Override
		protected void setUp() throws Exception {
			super.setUp();
			context = ZMQ.context(1);
			socket = context.socket(ZMQ.DEALER);
			socket.bind("tcp://127.0.0.1:7000");
		}
		@Override
		protected void tearDown() throws Exception {
			socket.close();
			context.term();
			super.tearDown();
		}
		
		public long timeGetEvents(int reps) {
			long val = 0;
			for (int i = 0; i < reps; i++) {
				val ^= socket.getEvents();
			}
			return val;
		}
		
		public static void main(String[] args) {
			CaliperMain.main(BenchmarkGetEvents.class, args);
		}
		
	}
	
}
