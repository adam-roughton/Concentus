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

public enum ServiceState implements ClusterState {
	
	CREATED(0),
	
	/**
	 * The initialisation phase of the service. The service should
	 * consume any initialisation data from the coordinator.
	 */
	INIT(1),
	
	/**
	 * The service should bind to sockets, and advertise its 
	 * service end-points.
	 */
	BIND(2),
	
	/**
	 * The service should connect to any service end-points it depends
	 * on.
	 */
	CONNECT(3),
	
	/**
	 * The service should start.
	 */
	START(4),
	
	/**
	 * The service should shutdown.
	 */
	SHUTDOWN(5)
	;
	private final int _code;
	
	private ServiceState(final int code) {
		_code = code;
	}
	
	public int code() {
		return _code;
	}
}
