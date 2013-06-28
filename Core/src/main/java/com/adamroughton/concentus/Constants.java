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

public final class Constants {

	/**
	 * The time length for each simulation tick in
	 * milliseconds. 
	 */
	public static final long TIME_STEP_IN_MS = 100L;
	
	/**
	 * The best effort time between metric events 
	 * from metric providers.
	 */
	public static final long METRIC_TICK = 1000L;
	
	public static final long METRIC_BUFFER_SECONDS = 60;
	
	/**
	 * The number of bytes used to uniquely identify
	 * a client.
	 */
	public static final int CLIENT_ID_LENGTH = 8;
	
	public static final int MSG_BUFFER_ENTRY_LENGTH = 512;
	
	public static final int DEFAULT_MSG_BUFFER_SIZE = 2048;
			
}
