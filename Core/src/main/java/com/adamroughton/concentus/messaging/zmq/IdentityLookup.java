package com.adamroughton.concentus.messaging.zmq;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.messaging.SocketIdentity;
import com.adamroughton.concentus.util.Util;

final class IdentityLookup {

	private final Clock _clock;
	private final Object2ObjectMap<String, SocketIdentity> _identityLookup = new Object2ObjectOpenHashMap<>();
	
	public IdentityLookup(Clock clock) {
		_clock = clock;
	}
	
	public void putIdentity(String connectionString, SocketIdentity identity) {
		synchronized(_identityLookup) {
			_identityLookup.put(connectionString, identity);
			_identityLookup.notifyAll();
		}
	}
	
	public SocketIdentity resolveIdentity(String connectionString, long timeout, TimeUnit unit) 
			throws InterruptedException, TimeoutException {
		long startTime = _clock.currentMillis();
		long deadline = unit.toMillis(timeout) + startTime;
		synchronized (_identityLookup) {
			long millisUntilDeadline = Util.millisUntil(deadline, _clock);
			while (!_identityLookup.containsKey(connectionString) && millisUntilDeadline > 0) {
				_identityLookup.wait(millisUntilDeadline);
				millisUntilDeadline = Util.millisUntil(deadline, _clock);
			}
			if (_identityLookup.containsKey(connectionString)) {
				return _identityLookup.get(connectionString);
			} else {
				throw new TimeoutException(String.format("Timed out waiting to resolve connection string '%s'", connectionString));
			}
		}
	}
	
	
}
