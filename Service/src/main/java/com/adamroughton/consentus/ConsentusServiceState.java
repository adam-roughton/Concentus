package com.adamroughton.consentus;

import com.adamroughton.consentus.cluster.worker.ClusterStateValue;

public enum ConsentusServiceState implements ClusterStateValue {
	/**
	 * The initial phase of the system. Components should
	 * request assignments.
	 */
	INIT(0),
	
	/**
	 * Components should read assignments, bind to sockets, 
	 * and advertise their services.
	 */
	BIND(1),
	
	/**
	 * Components should look up dependent services and connect
	 * to them.
	 */
	CONNECT(2),
	
	/**
	 * The system should start, sending updates and receiving events.
	 */
	START(3),
	
	/**
	 * The system should shutdown.
	 */
	SHUTDOWN(4)
	;
	private final int _code;
	
	private ConsentusServiceState(final int code) {
		_code = code;
	}
	
	public int code() {
		return _code;
	}

	@Override
	public int domain() {
		return 0;
	}
}
