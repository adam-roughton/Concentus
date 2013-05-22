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

import java.util.concurrent.TimeUnit;

public class DrivableClock implements Clock {

	private long _nanoTime = 0;
	
	@Override
	public long currentMillis() {
		return TimeUnit.NANOSECONDS.toMillis(_nanoTime);
	}

	@Override
	public long nanoTime() {
		return _nanoTime;
	}
	
	public void setTime(long time, TimeUnit unit) {
		_nanoTime = unit.toNanos(time);
	}

	public void advance(long timeDiff, TimeUnit unit) {
		_nanoTime += unit.toNanos(timeDiff);
	}
	
}
