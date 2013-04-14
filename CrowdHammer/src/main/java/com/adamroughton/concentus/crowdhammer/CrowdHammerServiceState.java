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
package com.adamroughton.concentus.crowdhammer;

import com.adamroughton.concentus.ConcentusServiceState;
import com.adamroughton.concentus.cluster.worker.ClusterStateValue;

public enum CrowdHammerServiceState implements ClusterStateValue {
	/**
	 * The initial phase when the test bed is brought online.
	 */
	INIT(0, null),
	
	/**
	 * Test components should read assignments, bind to sockets, 
	 * and advertise their services.
	 */
	BIND(1, null),
	
	/**
	 * Test components should look up dependent services and connect
	 * to them.
	 */
	CONNECT(2, null),
	
	/**
	 * The first phase of a test. Workers should request assignments. The system under test (SUT)
	 * should also be configured as if it were the {@link ConsentusServiceState#INIT}
	 * state.
	 */
	INIT_TEST(3, ConcentusServiceState.INIT),
	
	/**
	 * Workers should get their assignments and prepare for the test. The system under test (SUT)
	 * should also be configured as if it were the {@link ConsentusServiceState#BIND}
	 * state.
	 */
	SET_UP_TEST(4, ConcentusServiceState.BIND),
	
	/**
	 * The system under test (SUT) should internally connect all components
	 * as if the state were {@link ConsentusServiceState#CONNECT}.
	 */
	CONNECT_SUT(5, ConcentusServiceState.CONNECT),
	
	/**
	 * The system under test (SUT) should be started and be ready for
	 * the test as if the state were {@link ConsentusServiceState#START}.
	 */
	START_SUT(6, ConcentusServiceState.START),
	
	/**
	 * The test bed should start sending events to the system under test (SUT)
	 * and recording test metrics.
	 */
	EXEC_TEST(7, null),
	
	/**
	 * The test bed should stop sending events and finalise the collection of metrics.
	 * Metrics should be written to persistent storage if not already done so.
	 */
	STOP_SENDING_EVENTS(8, null),
	
	/**
	 * The current test should be torn down.
	 * Metrics should be written to persistent storage if not already done so. The 
	 * system under test (SUT) should be shutdown as if
	 * the state were {@link ConsentusServiceState#SHUTDOWN}.
	 */
	TEAR_DOWN(9, ConcentusServiceState.SHUTDOWN),
	
	/**
	 * The test harness should be shutdown.
	 */
	SHUTDOWN(10, null)
	;
	private final int _code;
	private final ConcentusServiceState _SUTState;
	
	private CrowdHammerServiceState(final int code, final ConcentusServiceState equivalentSUTState) {
		_code = code;
		_SUTState = equivalentSUTState;
	}
	
	public int code() {
		return _code;
	}
	
	public int domain() {
		return 1;
	}
	
	public ConcentusServiceState getEquivalentSUTState() {
		return _SUTState;
	}
}
