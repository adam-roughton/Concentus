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
package com.adamroughton.concentus.messaging.zmq;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerBridge;
import com.adamroughton.concentus.messaging.MessengerBridge.BridgeDelegate;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.messaging.TrackingMessengerDecorator;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.Mutex;

public class TrackingSocketManagerDecorator<TBuffer extends ResizingBuffer> implements ZmqSocketManager<TBuffer> {

	private final MetricContext _metricContext;
	private final ZmqSocketManager<TBuffer> _decoratedManager;
	private final Clock _clock;
	
	public TrackingSocketManagerDecorator(MetricContext metricContext, ZmqSocketManager<TBuffer> decoratedManager, Clock clock) {
		_metricContext = Objects.requireNonNull(metricContext);
		_decoratedManager = Objects.requireNonNull(decoratedManager);
		_clock = Objects.requireNonNull(clock);
	}
	
	@Override
	public BufferFactory<TBuffer> getBufferFactory() {
		return _decoratedManager.getBufferFactory();
	}
	
	@Override
	public MessageQueueFactory<TBuffer> newMessageQueueFactory(
			EventQueueFactory eventQueueFactory) {
		return _decoratedManager.newMessageQueueFactory(eventQueueFactory);
	}
	
	@Override
	public void close() throws IOException {
		_decoratedManager.close();
	}

	@Override
	public int create(int socketType, String name) {
		return _decoratedManager.create(socketType, name);
	}

	@Override
	public int create(int socketType, SocketSettings socketSettings, String name) {
		return _decoratedManager.create(socketType, socketSettings, name);
	}

	@Override
	public SocketSettings getSettings(int socketId) {
		return _decoratedManager.getSettings(socketId);
	}

	@Override
	public void updateSettings(int socketId, SocketSettings socketSettings) {
		_decoratedManager.updateSettings(socketId, socketSettings);
	}

	@Override
	public Mutex<Messenger<TBuffer>> getSocketMutex(int socketId) {
		return wrapMutex(_decoratedManager.getSocketMutex(socketId));
	}

	@Override
	public Mutex<Messenger<TBuffer>> createPollInSet(int... socketIds) {
		return wrapMutex(_decoratedManager.createPollInSet(socketIds));
	}
	
	private Mutex<Messenger<TBuffer>> wrapMutex(final Mutex<Messenger<TBuffer>> wrappedMutex) {
		return new Mutex<Messenger<TBuffer>>() {

			@Override
			public void runAsOwner(final OwnerDelegate<Messenger<TBuffer>> delegate) {
				wrappedMutex.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

					@Override
					public void asOwner(Messenger<TBuffer> messenger) {
						TrackingMessengerDecorator<TBuffer> trackingMessenger = new TrackingMessengerDecorator<>(_metricContext, messenger, _clock);
						delegate.asOwner(trackingMessenger);
					}
					
				});
			}

			@Override
			public boolean isOwned() {
				return wrappedMutex.isOwned();
			}

			@Override
			public void waitForRelease() throws InterruptedException {
				wrappedMutex.waitForRelease();
			}

			@Override
			public void waitForRelease(long timeout, TimeUnit unit)
					throws InterruptedException {
				wrappedMutex.waitForRelease(timeout, unit);
			}
			
		};
	}
	
	@Override
	public MessengerBridge<TBuffer> newBridge(
			int frontendSocketId, int backendSocketId,
			BridgeDelegate<TBuffer> bridgeDelegate,
			IncomingEventHeader frontendHeader,
			OutgoingEventHeader backendHeader) {
		return _decoratedManager.newBridge(frontendSocketId, backendSocketId,
				bridgeDelegate, frontendHeader, backendHeader);
	}

	@Override
	public MessengerBridge<TBuffer> newBridge(int frontendSocketId,
			int backendSocketId, BridgeDelegate<TBuffer> bridgeDelegate,
			int defaultBufferSize, IncomingEventHeader frontendHeader,
			OutgoingEventHeader backendHeader) {
		return _decoratedManager.newBridge(frontendSocketId, backendSocketId, 
				bridgeDelegate, defaultBufferSize, frontendHeader, backendHeader);
	}

	@Override
	public int connectSocket(int socketId, ServiceEndpoint endpoint) {
		return _decoratedManager.connectSocket(socketId, endpoint);
	}
	
	@Override
	public int connectSocket(int socketId, String address) {
		return _decoratedManager.connectSocket(socketId, address);
	}

	@Override
	public String disconnectSocket(int socketId, int connId) {
		return _decoratedManager.disconnectSocket(socketId, connId);
	}

	@Override
	public void destroySocket(int socketId) {
		_decoratedManager.destroySocket(socketId);
	}

	@Override
	public void destroyAllSockets() {
		_decoratedManager.destroyAllSockets();
	}

	@Override
	public SocketIdentity resolveIdentity(int socketId,
			ServiceEndpoint endpoint, long timeout, TimeUnit timeUnit)
			throws InterruptedException, TimeoutException,
			UnsupportedOperationException {
		return _decoratedManager.resolveIdentity(socketId, endpoint, timeout, timeUnit);
	}
	
	@Override
	public SocketIdentity resolveIdentity(int socketId,
			String connectionString, long timeout, TimeUnit timeUnit)
			throws InterruptedException, TimeoutException,
			UnsupportedOperationException {
		return _decoratedManager.resolveIdentity(socketId, connectionString, timeout, timeUnit);
	}

	@Override
	public int[] getBoundPorts(int socketId) {
		return _decoratedManager.getBoundPorts(socketId);
	}
	
	@Override
	public int getBoundPort(int socketId) {
		return _decoratedManager.getBoundPort(socketId);
	}

}
