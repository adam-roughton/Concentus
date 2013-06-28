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

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.output.NullOutputStream;
import org.zeromq.ZMQ;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.clienthandler.ClientHandlerService;
import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.adamroughton.concentus.cluster.data.TestRunInfo;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.adamroughton.concentus.crowdhammer.config.StubCrowdHammerConfiguration;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.StubMessenger;
import com.adamroughton.concentus.messaging.StubMessenger.FakeRecvDelegate;
import com.adamroughton.concentus.messaging.StubMessenger.FakeSendDelegate;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;
import com.adamroughton.concentus.messaging.events.ClientUpdateEvent;
import com.adamroughton.concentus.messaging.events.ConnectResponseEvent;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.messaging.zmq.StubSocketManager;
import com.adamroughton.concentus.messaging.zmq.StubSocketManager.StubMessengerConfigurator;
import com.adamroughton.concentus.metric.LogMetricContext;
import com.adamroughton.concentus.metric.NullMetricContext;
import com.netflix.curator.framework.api.CuratorWatcher;

import static com.adamroughton.concentus.crowdhammer.CrowdHammerServiceState.*;

public class WorkerPerfTest {
	
	int clientCount;
	int updateCount;
	long updateTickMillis;
	
	private final PrintStream _consolePrintStream;
	private Thread _clientHandlerThread;
	private AtomicBoolean _testStartFlag = new AtomicBoolean(false);
	private CountDownLatch _endLatch;
	private WorkerService _worker;
	private ClusterWorkerHandle _clusterHandle;
	private CyclicBarrier _testThreadBarrier;
	
	public WorkerPerfTest(PrintStream consolePrintStream) {
		_consolePrintStream = Objects.requireNonNull(consolePrintStream);
	}
	
	public void setUp() throws Exception {
		_endLatch = new CountDownLatch(1);
		
		final AtomicBoolean sendConnectionResponses = new AtomicBoolean(false);
		final FakeRecvDelegate clientRecvDelegate = new FakeRecvDelegate() {
			
			ConnectResponseEvent _connectResEvent = new ConnectResponseEvent();
			ClientUpdateEvent _updateEvent = new ClientUpdateEvent();
			IncomingEventHeader _header = new IncomingEventHeader(0, 1);
			
			boolean hasStopped = false;
			long lastUpdateTime = 0;
			
			long initSeq = 0;
			
			@Override
			public boolean fakeRecv(int[] endPointIds, long recvSeq, byte[] eventBuffer,
					IncomingEventHeader header, boolean isBlocking) {
				if (recvSeq >= updateCount * clientCount + clientCount + initSeq) {
					if (!hasStopped) {
						_endLatch.countDown();
						hasStopped = true;
					}
					return false;
				} else if (!sendConnectionResponses.get()) {
					initSeq++;
					return false;
				} else {
					long seq = recvSeq - initSeq;
					int clientId = (int) (seq % clientCount);
					long now = System.currentTimeMillis();
					
					int cursor = _header.getEventOffset();
					ByteArrayBackedEvent event;
					if (seq < clientCount) {
						event = _connectResEvent;
						_connectResEvent.setBackingArray(eventBuffer, cursor);
						_connectResEvent.setCallbackBits(clientId);
						_connectResEvent.setClientId(clientId);
						_connectResEvent.setResponseCode(ConnectResponseEvent.RES_OK);
					} else if (_testStartFlag.get() && now - lastUpdateTime > updateTickMillis) {
						lastUpdateTime = now;
						
						event = _updateEvent;
						_updateEvent.setBackingArray(eventBuffer, cursor);
						_updateEvent.setClientId(clientId);
						_updateEvent.setSimTime(System.currentTimeMillis());
						_updateEvent.setUpdateId((seq - clientCount) / clientCount);
						_updateEvent.setHighestInputActionId((seq - clientCount) / clientCount * 10);
						_updateEvent.setUsedLength(100);
					} else {
						initSeq++;
						return false;
					}
					
					_header.setIsValid(eventBuffer, true);
					_header.setSocketId(eventBuffer, endPointIds[0]);
					_header.setRecvTime(eventBuffer, System.currentTimeMillis());
					_header.setSocketId(eventBuffer, 0);
					event.writeEventTypeId();
					_header.setSegmentMetaData(eventBuffer, 0, cursor, event.getEventSize());
					return true;
				}
			}
		};
		
		final FakeSendDelegate clientSendDelegate = new FakeSendDelegate() {

			long _messageCount = 0;
			boolean hasSentSignal = false;
			
			@Override
			public boolean fakeSend(long sendSeq, byte[] eventBuffer,
					OutgoingEventHeader header, boolean isBlocking) {
				if (++_messageCount >= clientCount && !hasSentSignal) {
					sendConnectionResponses.set(true);
					hasSentSignal = true;
				}
				return true;
			}
			
		};
		
		ConcentusHandle<CrowdHammerConfiguration> concentusHandle = new ConcentusHandle<CrowdHammerConfiguration>(new InstanceFactory<SocketManager>() {

			@Override
			public SocketManager newInstance() {
				return new StubSocketManager(new StubMessengerConfigurator() {
					
					@Override
					public void onStubMessengerCreation(int socketId, StubMessenger messenger,
							int socketType, SocketSettings settings) {
						if (socketType == ZMQ.DEALER) {
							messenger.setFakeRecvDelegate(clientRecvDelegate);
							messenger.setFakeSendDelegate(clientSendDelegate);
						}
					}
				});
			}
			
		}, new StandardEventQueueFactory(new NullMetricContext()), new DefaultClock(), new StubCrowdHammerConfiguration(30), InetAddress.getLoopbackAddress(), "127.0.0.1:50000");
		_worker = new WorkerService(concentusHandle, 100000, new LogMetricContext(Constants.METRIC_TICK, TimeUnit.SECONDS.toMillis(Constants.METRIC_BUFFER_SECONDS), new DefaultClock()));
		
		_clusterHandle = new ClusterWorkerHandle() {
			
			private final UUID _id = UUID.fromString("abababab-abab-abab-abab-abababababab");
			private final String[] _clientHandlerService = new String[] {"127.0.0.1:9090"};
			
			@Override
			public void unregisterService(String serviceType) {
			}
			
			@Override
			public void signalReady() {
			}
			
			@Override
			public void requestAssignment(String serviceType, byte[] requestBytes) {
			}
			
			@Override
			public void registerService(String serviceType, String address) {
			}
			
			@Override
			public String getServiceAtRandom(String serviceType) {
				if (serviceType == CanonicalStateService.SERVICE_TYPE) {
					return _clientHandlerService[0];
				} else {
					throw new RuntimeException(String.format("Unknown service type '%s'", serviceType));
				}
			}
			
			@Override
			public UUID getMyId() {
				return _id;
			}
			
			int getAssignmentInvocationCount = 0;
			
			@Override
			public byte[] getAssignment(String serviceType) {
				getAssignmentInvocationCount++;
				
				if (getAssignmentInvocationCount == 1) {
					byte[] workerIdRequest = new byte[8];
					MessageBytesUtil.writeLong(workerIdRequest, 0, 0);
					return workerIdRequest;
				} else if (getAssignmentInvocationCount == 2) {
					byte[] clientAllocationRequest = new byte[4];
					MessageBytesUtil.writeInt(clientAllocationRequest, 0, clientCount);
					return clientAllocationRequest;
				} else {
					throw new RuntimeException("Unexpected get assignment request");
				}
			}
			
			@Override
			public String[] getAllServices(String serviceType) {
				if (serviceType == ClientHandlerService.SERVICE_TYPE) {
					return _clientHandlerService;
				} else {
					throw new RuntimeException(String.format("Unknown service type '%s'", serviceType));
				}
			}
			
			@Override
			public void deleteAssignmentRequest(String serviceType) {
			}

			@Override
			public TestRunInfo getCurrentRunInfo() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void registerAsMetricPublisher(String type,
					String pubAddress, String metaDataReqAddress) {
			}

			@Override
			public List<MetricPublisherInfo> getMetricPublishers() {
				return Collections.singletonList(new MetricPublisherInfo(_id, "", "", ""));
			}

			@Override
			public List<MetricPublisherInfo> getMetricPublishers(
					CuratorWatcher watcher) {
				return Collections.singletonList(new MetricPublisherInfo(_id, "", "", ""));
			}
		};
		_testThreadBarrier = new CyclicBarrier(2);
		_clientHandlerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					for (CrowdHammerServiceState state : Arrays.asList(INIT, BIND, CONNECT, INIT_TEST, SET_UP_TEST, CONNECT_SUT, START_SUT, EXEC_TEST)) {
						_worker.onStateChanged(state, _clusterHandle);
					}
					_testThreadBarrier.await();
					for (CrowdHammerServiceState state : Arrays.asList(TEAR_DOWN, SHUTDOWN)) {						
						_worker.onStateChanged(state, _clusterHandle);
					}
				} catch (Exception e) {
					e.printStackTrace(_consolePrintStream);
				}
				
			}
			
		});
		_clientHandlerThread.start();
	}
	
	public void timeWorker() {
		_testStartFlag.set(true);
		try {
			if (!_endLatch.await(600, TimeUnit.SECONDS)) {
				throw new RuntimeException("The test timed out");				
			}
		} catch (InterruptedException eInterrupted) {
			throw new RuntimeException(eInterrupted);
		}
	}
	
	public void tearDown() throws Exception {
		_testThreadBarrier.await();
		_clientHandlerThread.join();
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Press enter to start");
		System.in.read();
		PrintStream consoleStream = System.out;
		try (PrintStream nullPrintStream = new PrintStream(new NullOutputStream())) {
		
			//System.setOut(nullPrintStream);
			//System.setErr(nullPrintStream);
			
			while(true) {
				WorkerPerfTest perfTest = new WorkerPerfTest(consoleStream);
				perfTest.updateCount = 100;
				perfTest.clientCount = 16384;
				perfTest.updateTickMillis = 100;
				perfTest.setUp();
				
				long startTime = System.nanoTime();
				perfTest.timeWorker();
				long duration = System.nanoTime() - startTime;
				double throughput = ((double) perfTest.updateCount / duration) * TimeUnit.SECONDS.toNanos(1);
				consoleStream.println(String.format("%f updates/s", throughput));
				
				perfTest.tearDown();
			}
		}
	}
	
}
