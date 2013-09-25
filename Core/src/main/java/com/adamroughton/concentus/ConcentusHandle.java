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
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.esotericsoftware.minlog.Log;

public class ConcentusHandle implements FatalExceptionCallback {
	
	private final AtomicBoolean _isShuttingDown = new AtomicBoolean(false);
	
	private final Clock _clock;
	private final InetAddress _networkAddress;
	private final String _zooKeeperAddress;
	
	private final Set<String> _traceFlagLookup;
	
	public ConcentusHandle(
			Clock clock, 
			InetAddress networkAddress,
			String zooKeeperAddress,
			Set<String> traceFlagLookup) {
		_clock = Objects.requireNonNull(clock);
		_networkAddress = Objects.requireNonNull(networkAddress);
		_zooKeeperAddress = Objects.requireNonNull(zooKeeperAddress);
		_traceFlagLookup = Objects.requireNonNull(traceFlagLookup);
	}
	
	public Clock getClock() {
		return _clock;
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
	
	public boolean shouldTrace(String componentType) {
		return _traceFlagLookup.contains(componentType);
	}

	public void shutdown() {
		if (!_isShuttingDown.getAndSet(true)) {
			System.exit(0);
		}
	}

}
