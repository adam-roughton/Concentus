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
package com.adamroughton.concentus.util;

import java.util.concurrent.TimeUnit;

public interface Mutex<TObject> {
	
	/**
	 * Executes the provided runnable object with exclusive
	 * access to the object, releasing access when the runnable
	 * completes.
	 * 
	 * @param delegate the delegate to execute
	 * @throws IllegalStateException if the Mutex is already owned by another
	 * thread
	 */
	void runAsOwner(OwnerDelegate<TObject> delegate);
	
	boolean isOwned();
	
	void waitForRelease() throws InterruptedException;
	
	void waitForRelease(long timeout, TimeUnit unit) throws InterruptedException;
	
	public interface OwnerDelegate<TObject> {
		void asOwner(TObject item);
	}
}
