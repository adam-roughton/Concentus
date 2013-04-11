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
package com.adamroughton.concentus.disruptor;

import java.util.Objects;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.lmax.disruptor.ExceptionHandler;

public class FailFastExceptionHandler implements ExceptionHandler {

	private final String _disruptorName;
	private final FatalExceptionCallback _fatalExceptionCallback;
	
	public FailFastExceptionHandler(final String disruptorName, FatalExceptionCallback fatalExceptionCallback) {
		_disruptorName = disruptorName;
		_fatalExceptionCallback = Objects.requireNonNull(fatalExceptionCallback);
	}
	
	@Override
	public void handleEventException(Throwable ex, long sequence,
			Object event) {
		signalException(String.format("sequence %d", sequence), ex);
	}

	@Override
	public void handleOnStartException(Throwable ex) {
		signalException("start", ex);
	}

	@Override
	public void handleOnShutdownException(Throwable ex) {
		signalException("shutdown", ex);
	}
	
	private void signalException(final String location, final Throwable ex) {
		_fatalExceptionCallback.signalFatalException(
				new RuntimeException(String.format("Fatal exception on disruptor %s on %s",
						_disruptorName, location), ex));
	}
}