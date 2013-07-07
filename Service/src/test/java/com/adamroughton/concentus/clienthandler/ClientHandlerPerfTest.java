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

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.DefaultClock;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.canonicalstate.CanonicalStateService;
import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.adamroughton.concentus.cluster.data.TestRunInfo;
import com.adamroughton.concentus.cluster.worker.ClusterWorkerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.configuration.StubConfiguration;
import com.adamroughton.concentus.disruptor.MetricTrackingEventQueueFactory;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.StubMessenger;
import com.adamroughton.concentus.messaging.StubMessenger.FakeRecvDelegate;
import com.adamroughton.concentus.messaging.StubMessenger.FakeSendDelegate;
import com.adamroughton.concentus.messaging.events.ByteArrayBackedEvent;
import com.adamroughton.concentus.messaging.events.ClientConnectEvent;
import com.adamroughton.concentus.messaging.events.ClientHandlerEntry;
import com.adamroughton.concentus.messaging.events.ClientInputEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateEvent;
import com.adamroughton.concentus.messaging.events.StateUpdateInfoEvent;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.messaging.zmq.StubSocketManager;
import com.adamroughton.concentus.messaging.zmq.StubSocketManager.StubMessengerConfigurator;
import com.adamroughton.concentus.messaging.zmq.TrackingSocketManagerDecorator;
import com.adamroughton.concentus.metric.LogMetricContext;
import com.netflix.curator.framework.api.CuratorWatcher;

import static com.adamroughton.concentus.ConcentusServiceState.*;

public class ClientHandlerPerfTest {
	
	int clientCount;
	int messageCount;
	long updateTickPeriod;
	boolean fakeStateUpdates;
	long msgSendDelayNanos;
	
	private final PrintStream _consolePrintStream;
	private Thread _clientHandlerThread;
	private CountDownLatch _startLatch;
	private CountDownLatch _endLatch;
	private ClientHandlerService _clientHandler;
	private ClusterWorkerHandle _clusterHandle;
	private CyclicBarrier _testThreadBarrier;
	
	public ClientHandlerPerfTest(PrintStream consolePrintStream) {
		_consolePrintStream = Objects.requireNonNull(consolePrintStream);
	}
	
	public void setUp() throws Exception {
		_startLatch = new CountDownLatch(1);
		_endLatch = new CountDownLatch(1);
		
		final FakeRecvDelegate routerRecvDelegate = new FakeRecvDelegate() {
			
			ClientConnectEvent _connectEvent = new ClientConnectEvent();
			ClientInputEvent _inputEvent = new ClientInputEvent();
			IncomingEventHeader _header = new IncomingEventHeader(0, 2);
			
			boolean hasStarted = false;
			boolean hasStopped = false;
			
			@Override
			public boolean fakeRecv(int[] endPointIds, long recvSeq, byte[] eventBuffer,
					IncomingEventHeader header, boolean isBlocking) {
				if (recvSeq >= messageCount + clientCount) {
					if (!hasStopped) {
						_endLatch.countDown();
						hasStopped = true;
					}
					return false;
				} else {
					int clientId = (int) (recvSeq % clientCount);
					
					_header.setIsValid(eventBuffer, true);
					_header.setSocketId(eventBuffer, endPointIds[0]);
					_header.setRecvTime(eventBuffer, System.currentTimeMillis());
					int cursor = _header.getEventOffset();
					_header.setSegmentMetaData(eventBuffer, 0, cursor, 4);
					MessageBytesUtil.writeInt(eventBuffer, cursor, clientId);
					cursor += 4;
					ByteArrayBackedEvent event;
					if (recvSeq < clientCount) {
						event = _connectEvent;
						_connectEvent.setBackingArray(eventBuffer, cursor);
						_connectEvent.setCallbackBits(recvSeq);
					} else {
						if (!hasStarted) {
							try {
								_startLatch.await();
							} catch (InterruptedException eInterrupted) {
								Thread.currentThread().interrupt();
								return false;
							}
							hasStarted = true;
						}
						event = _inputEvent;
						_inputEvent.setBackingArray(eventBuffer, cursor);
						_inputEvent.setClientId(clientId);
						_inputEvent.setClientActionId(recvSeq / clientCount);
						_inputEvent.setUsedLength(0);
					}
					event.writeEventTypeId();
					_header.setSegmentMetaData(eventBuffer, 1, cursor, event.getEventSize());
					return true;
				}
			}
		};
		
		final FakeRecvDelegate updateRecvDelegate = new FakeRecvDelegate() {
			
			long _lastUpdateTime = 0;
			StateUpdateEvent _updateEvent = new StateUpdateEvent();
			StateUpdateInfoEvent _infoEvent = new StateUpdateInfoEvent();
			long _updateId = 0;
			boolean _nextShouldBeInfo = false;
			
			@Override
			public boolean fakeRecv(int[] endPointIds, long recvSeq, byte[] eventBuffer,
					IncomingEventHeader header, boolean isBlocking) {
				long now = System.currentTimeMillis();
				if (now - _lastUpdateTime > updateTickPeriod) {
					header.setIsValid(eventBuffer, true);
					header.setSocketId(eventBuffer, endPointIds[0]);
					int cursor = header.getEventOffset();
					MessageBytesUtil.writeInt(eventBuffer, cursor, _updateEvent.getEventTypeId());
					header.setSegmentMetaData(eventBuffer, 0, cursor, 4);
					cursor += 4;
					_updateEvent.setBackingArray(eventBuffer, cursor);
					_updateEvent.writeEventTypeId();
					_updateEvent.setSimTime(now);
					_updateEvent.setUpdateId(_updateId++);
					_updateEvent.setUsedLength(50);
					header.setSegmentMetaData(eventBuffer, 1, cursor, _updateEvent.getEventSize());
					_updateEvent.releaseBackingArray();
					_nextShouldBeInfo = true;
					_lastUpdateTime = now;
					return true;
				} else if (_nextShouldBeInfo) {
					header.setIsValid(eventBuffer, true);
					header.setSocketId(eventBuffer, endPointIds[0]);
					int cursor = header.getEventOffset();
					MessageBytesUtil.writeInt(eventBuffer, cursor, _infoEvent.getEventTypeId());
					header.setSegmentMetaData(eventBuffer, 0, cursor, 4);
					cursor += 4;
					_infoEvent.setBackingArray(eventBuffer, cursor);
					_infoEvent.writeEventTypeId();
					_infoEvent.setUpdateId(_updateId - 1);
					_infoEvent.setEntryCount(1);
					_infoEvent.setHandlerEntry(0, new ClientHandlerEntry(0, recvSeq));
					header.setSegmentMetaData(eventBuffer, 1, cursor, _infoEvent.getEventSize());
					_infoEvent.releaseBackingArray();
					
					_nextShouldBeInfo = false;
					return true;
				} else {
					return false;
				}
			}
		};
		
		final FakeSendDelegate sendDelegate = new FakeSendDelegate() {
			
			@Override
			public boolean fakeSend(long sendSeq, byte[] eventBuffer,
					OutgoingEventHeader header, boolean isBlocking) {
				if (!header.isValid(eventBuffer)) return true;
				
				long startTime = System.nanoTime();
				long deadline = startTime + msgSendDelayNanos;
				while (System.nanoTime() < deadline);
				
				return true;
			}
		};
		
		final LogMetricContext metricContext = new LogMetricContext(Constants.METRIC_TICK, TimeUnit.SECONDS.toMillis(Constants.METRIC_BUFFER_SECONDS), new DefaultClock());
		metricContext.start();
		
		ConcentusHandle<Configuration> concentusHandle = new ConcentusHandle<Configuration>(new InstanceFactory<SocketManager>() {

			@Override
			public SocketManager newInstance() {
				SocketManager stubManager = new StubSocketManager(new StubMessengerConfigurator() {
					
					@Override
					public void onStubMessengerCreation(int socketId, StubMessenger messenger,
							int socketType, SocketSettings settings) {
						if (socketType == ZMQ.ROUTER) {
							messenger.setFakeRecvDelegate(routerRecvDelegate);
						} else if (socketType == ZMQ.PUB) {
							messenger.setFakeSendDelegate(sendDelegate);
						} else if (socketType == ZMQ.SUB) {
							if (fakeStateUpdates) {
								messenger.setFakeRecvDelegate(updateRecvDelegate);
							} else {
								messenger.setFakeRecvDelegate(new FakeRecvDelegate() {

									@Override
									public boolean fakeRecv(int[] endPointIds, long recvSeq, byte[] eventBuffer,
											IncomingEventHeader header, boolean isBlocking) {
										LockSupport.park();
										return false;
									}
								});
							}
						}
					}
				});
				return new TrackingSocketManagerDecorator(metricContext, stubManager, new DefaultClock());
			}
			
		}, new StandardEventQueueFactory(metricContext) /* new MetricTrackingEventQueueFactory(metricContext, new DefaultClock()) */, new DefaultClock(), new StubConfiguration(), InetAddress.getLoopbackAddress(), "127.0.0.1:50000");
		
		_clientHandler = new ClientHandlerService(concentusHandle, metricContext);
		
		_clusterHandle = new ClusterWorkerHandle() {
			
			private final UUID _id = UUID.fromString("abababab-abab-abab-abab-abababababab");
			private final String[] _canonicalStateService = new String[] {"127.0.0.1:9090"};
			
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
					return _canonicalStateService[0];
				} else {
					throw new RuntimeException(String.format("Unknown service type '%s'", serviceType));
				}
			}
			
			@Override
			public UUID getMyId() {
				return _id;
			}
			
			@Override
			public byte[] getAssignment(String serviceType) {
				if (serviceType == ClientHandlerService.SERVICE_TYPE) {
					byte[] clientHandlerId = new byte[4];
					MessageBytesUtil.writeInt(clientHandlerId, 0, 0);
					return clientHandlerId;
				} else {
					throw new RuntimeException(String.format("Unknown service type '%s'", serviceType));
				}
			}
			
			@Override
			public String[] getAllServices(String serviceType) {
				if (serviceType == CanonicalStateService.SERVICE_TYPE) {
					return _canonicalStateService;
				} else {
					throw new RuntimeException(String.format("Unknown service type '%s'", serviceType));
				}
			}
			
			@Override
			public void deleteAssignmentRequest(String serviceType) {
			}

			@Override
			public TestRunInfo getCurrentRunInfo() {
				return null;
			}

			@Override
			public void registerAsMetricPublisher(String type,
					String pubAddress, String metaDataReqAddress) {
			}

			@Override
			public List<MetricPublisherInfo> getMetricPublishers() {
				return null;
			}

			@Override
			public List<MetricPublisherInfo> getMetricPublishers(
					CuratorWatcher watcher) {
				return null;
			}
		};
		_testThreadBarrier = new CyclicBarrier(2);
		_clientHandlerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					for (ConcentusServiceState state : Arrays.asList(INIT, BIND, CONNECT, START)) {
						_clientHandler.onStateChanged(state, _clusterHandle);
					}
					_testThreadBarrier.await();
					_clientHandler.onStateChanged(SHUTDOWN, _clusterHandle);
				} catch (Exception e) {
					e.printStackTrace(_consolePrintStream);
				}
				
			}
			
		});
		_clientHandlerThread.start();
	}
	
	public void timeClientHandler() {
		_startLatch.countDown();
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
		/*
		PrintStream nullPrintStream = new PrintStream(new NullOutputStream());
		System.setOut(nullPrintStream);
		System.setErr(nullPrintStream);*/
		
		while(true) {
			ClientHandlerPerfTest perfTest = new ClientHandlerPerfTest(consoleStream);
			perfTest.messageCount = 100000000;
			perfTest.clientCount = 10000;
			perfTest.updateTickPeriod = 100;
			perfTest.msgSendDelayNanos = 1000;
			perfTest.fakeStateUpdates = true;
			perfTest.setUp();
			
			long startTime = System.nanoTime();
			perfTest.timeClientHandler();
			long duration = System.nanoTime() - startTime;
			double throughput = ((double) perfTest.messageCount / duration) * TimeUnit.SECONDS.toNanos(1);
			consoleStream.println(String.format("%f msgs/s", throughput));
			
			perfTest.tearDown();
		}
	}
	
}
