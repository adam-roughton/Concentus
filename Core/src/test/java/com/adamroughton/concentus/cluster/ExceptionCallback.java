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
package com.adamroughton.concentus.cluster;

import java.util.concurrent.atomic.AtomicReference;

import com.adamroughton.concentus.FatalExceptionCallback;

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