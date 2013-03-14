package com.adamroughton.consentus;

import java.util.concurrent.atomic.AtomicBoolean;

import com.esotericsoftware.minlog.Log;

public class DefaultProcessCallback implements ConsentusProcessCallback {

	private final AtomicBoolean _isShuttingDown = new AtomicBoolean(false);
	
	@Override
	public void signalFatalException(Throwable exception) {
		Log.error("Fatal exception:", exception);
		if (!_isShuttingDown.getAndSet(true)) {
			System.exit(1);
		}
	}

	@Override
	public void shutdown() {
		if (!_isShuttingDown.getAndSet(true)) {
			System.exit(0);
		}
	}

}
