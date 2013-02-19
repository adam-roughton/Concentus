package com.adamroughton.consentus;

public interface FatalExceptionCallback {

	void signalFatalException(final Throwable exception);
	
}
