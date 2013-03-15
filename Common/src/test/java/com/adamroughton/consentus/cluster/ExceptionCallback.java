package com.adamroughton.consentus.cluster;

import java.util.concurrent.atomic.AtomicReference;

import com.adamroughton.consentus.FatalExceptionCallback;

public class ExceptionCallback implements FatalExceptionCallback {

	private final AtomicReference<Throwable> _thrownException = 
			new AtomicReference<Throwable>(null);
	
	@Override
	public void signalFatalException(Throwable exception) {
		// only capture the first
		_thrownException.compareAndSet(null, exception);
	}
	
	public void throwAnyExceptions() throws Throwable {
		Throwable thrownException = _thrownException.get();
		if (thrownException != null) {
			throw thrownException;
		}
	}
	
}