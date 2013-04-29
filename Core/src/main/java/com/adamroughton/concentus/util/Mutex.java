package com.adamroughton.concentus.util;

import java.util.concurrent.TimeUnit;

public interface Mutex<TObject> {
	
	/**
	 * Executes the provided runnable object with exclusive
	 * access to the object, releasing access when the runnable
	 * completes.
	 * 
	 * @param delegate the delegate to execute
	 */
	void runAsOwner(OwnerDelegate<TObject> delegate);
	
	boolean isOwned();
	
	void waitForRelease() throws InterruptedException;
	
	void waitForRelease(long timeout, TimeUnit unit) throws InterruptedException;
	
	public interface OwnerDelegate<TObject> {
		void asOwner(TObject item);
	}
}
