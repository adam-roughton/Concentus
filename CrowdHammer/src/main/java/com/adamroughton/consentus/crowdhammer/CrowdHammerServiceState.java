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
package com.adamroughton.consentus.crowdhammer;

import com.adamroughton.consentus.ConsentusServiceState;
import com.adamroughton.consentus.cluster.worker.ClusterStateValue;

public enum CrowdHammerServiceState implements ClusterStateValue {
	/**
	 * The initial phase when the test bed is brought online.
	 */
	INIT(0, null),
	
	/**
	 * The first phase of a test. Workers should request assignments. The system under test (SUT)
	 * should also be configured as if it were the {@link ConsentusServiceState#INIT}
	 * state.
	 */
	INIT_TEST(1, ConsentusServiceState.INIT),
	
	/**
	 * Workers should get their assignments and prepare for the test. The system under test (SUT)
	 * should also be configured as if it were the {@link ConsentusServiceState#BIND}
	 * state.
	 */
	SET_UP_TEST(2, ConsentusServiceState.BIND),
	
	/**
	 * The system under test (SUT) should internally connect all components
	 * as if the state were {@link ConsentusServiceState#CONNECT}.
	 */
	CONNECT_SUT(3, ConsentusServiceState.CONNECT),
	
	/**
	 * The system under test (SUT) should be started and be ready for
	 * the test as if the state were {@link ConsentusServiceState#START}.
	 */
	START_SUT(4, ConsentusServiceState.START),
	
	/**
	 * The test bed should start sending events to the system under test (SUT)
	 * and recording test metrics.
	 */
	EXEC_TEST(5, null),
	
	/**
	 * The test bed should stop sending events and finalise the collection of metrics.
	 * Metrics should be written to persistent storage if not already done so.
	 */
	STOP_SENDING_EVENTS(6, null),
	
	/**
	 * The test bed should be torn down.
	 * Metrics should be written to persistent storage if not already done so. The 
	 * system under test (SUT) should be shutdown as if
	 * the state were {@link ConsentusServiceState#SHUTDOWN}.
	 */
	TEAR_DOWN(7, ConsentusServiceState.SHUTDOWN),
	
	/**
	 * The test harness should be shutdown.
	 */
	SHUTDOWN(8, null)
	;
	private final int _code;
	private final ConsentusServiceState _SUTState;
	
	private CrowdHammerServiceState(final int code, final ConsentusServiceState equivalentSUTState) {
		_code = code;
		_SUTState = equivalentSUTState;
	}
	
	public int code() {
		return _code;
	}
	
	public int domain() {
		return 1;
	}
	
	public ConsentusServiceState getEquivalentSUTState() {
		return _SUTState;
	}
}
