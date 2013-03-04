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
package com.adamroughton.consentus;

import java.util.UUID;

public final class Constants {

	/**
	 * The time length for each simulation tick in
	 * milliseconds. 
	 */
	public static final long TIME_STEP_IN_MS = 100L;
	
	/**
	 * The number of bytes used to uniquely identify
	 * a client.
	 */
	public static final int CLIENT_ID_LENGTH = 16;
	
	/**
	 * 
	 */
	public static final UUID INTERNAL_SENDER_ID = UUID.fromString("97b26470-81e9-11e2-9e96-0800200c9a66");
	
	public static final int MSG_BUFFER_LENGTH = 512;
			
}
