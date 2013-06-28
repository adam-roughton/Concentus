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

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.TrackingMessengerDecorator;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.Mutex;

public class TrackingSocketManagerDecorator implements SocketManager {

	private final MetricContext _metricContext;
	private final SocketManager _decoratedManager;
	private final Clock _clock;
	
	public TrackingSocketManagerDecorator(MetricContext metricContext, SocketManager decoratedManager, Clock clock) {
		_metricContext = Objects.requireNonNull(metricContext);
		_decoratedManager = Objects.requireNonNull(decoratedManager);
		_clock = Objects.requireNonNull(clock);
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
	public Mutex<Messenger> getSocketMutex(int socketId) {
		return wrapMutex(_decoratedManager.getSocketMutex(socketId));
	}

	@Override
	public Mutex<Messenger> createPollInSet(int... socketIds) {
		return wrapMutex(_decoratedManager.createPollInSet(socketIds));
	}
	
	private Mutex<Messenger> wrapMutex(final Mutex<Messenger> wrappedMutex) {
		return new Mutex<Messenger>() {

			@Override
			public void runAsOwner(final OwnerDelegate<Messenger> delegate) {
				wrappedMutex.runAsOwner(new OwnerDelegate<Messenger>() {

					@Override
					public void asOwner(Messenger messenger) {
						TrackingMessengerDecorator trackingMessenger = new TrackingMessengerDecorator(_metricContext, messenger, _clock);
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

}
