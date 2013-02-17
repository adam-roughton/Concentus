package com.adamroughton.consentus;

public interface ConcentusProcessCallback {

	/**
	 * Requests that the exception be handled.
	 * @param ex
	 */
	void handleException(Throwable ex);
	
	void shutdown();
	
}
