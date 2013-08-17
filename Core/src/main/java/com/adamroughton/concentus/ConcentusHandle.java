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
package com.adamroughton.concentus;

import java.net.InetAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.ResizingBuffer;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.esotericsoftware.minlog.Log;

public class ConcentusHandle<TConfig extends Configuration, TBuffer extends ResizingBuffer> implements FatalExceptionCallback {
	
	private final AtomicBoolean _isShuttingDown = new AtomicBoolean(false);
	
	private final InstanceFactory<SocketManager<TBuffer>> _socketManager;
	private final EventQueueFactory _eventQueueFactory;
	private final Clock _clock;
	private final TConfig _config;
	private final InetAddress _networkAddress;
	private final String _zooKeeperAddress;
	
	public ConcentusHandle(
			InstanceFactory<SocketManager<TBuffer>> socketManagerFactory,
			EventQueueFactory eventQueueFactory,
			Clock clock, 
			TConfig config, 
			InetAddress networkAddress,
			String zooKeeperAddress) {
		_socketManager = Objects.requireNonNull(socketManagerFactory);
		_eventQueueFactory = Objects.requireNonNull(eventQueueFactory);
		_clock = Objects.requireNonNull(clock);
		_config = Objects.requireNonNull(config);
		_networkAddress = Objects.requireNonNull(networkAddress);
		_zooKeeperAddress = Objects.requireNonNull(zooKeeperAddress);
	}
	
	public SocketManager<TBuffer> newSocketManager() {
		return _socketManager.newInstance();
	}
	
	public EventQueueFactory getEventQueueFactory() {
		return _eventQueueFactory;
	}
	
	public Clock getClock() {
		return _clock;
	}
	
	public TConfig getConfig() {
		return _config;
	}
	
	public InetAddress getNetworkAddress() {
		return _networkAddress;
	}
	
	public String getZooKeeperAddress() {
		return _zooKeeperAddress;
	}
	
	@Override
	public void signalFatalException(Throwable exception) {
		Log.error("Fatal exception:", exception);
		if (!_isShuttingDown.getAndSet(true)) {
			System.exit(1);
		}
	}

	public void shutdown() {
		if (!_isShuttingDown.getAndSet(true)) {
			System.exit(0);
		}
	}
	
}
