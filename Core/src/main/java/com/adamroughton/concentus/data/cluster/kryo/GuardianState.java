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
package com.adamroughton.concentus.data.cluster.kryo;

public enum GuardianState implements ClusterState {
	
	CREATED(0),
	
	/**
	 * The guardian should be in a state to accept
	 * new run requests
	 */
	READY(1),
	
	/**
	 * The guardian should run the assigned service
	 */
	RUN(2),
	
	/**
	 * The guardian should shutdown.
	 */
	SHUTDOWN(3)
	;
	private final int _code;
	
	private GuardianState(final int code) {
		_code = code;
	}
	
	public int code() {
		return _code;
	}
}
