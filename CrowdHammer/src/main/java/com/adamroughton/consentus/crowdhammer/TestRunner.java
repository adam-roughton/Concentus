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

import java.util.List;

import com.adamroughton.consentus.cluster.coordinator.ClusterCoordinator;

public final class TestRunner implements Runnable {

	private final ClusterCoordinator _cluster;
	private final int _testRunDuration;
	
	private long _testCheck;
	
	public TestRunner(ClusterCoordinator cluster, int testRunDuration, int[] testClientCounts) {
		_cluster = cluster;
		_testRunDuration = testRunDuration;
	}
	
	@Override
	public void run() {
		// iterate through client counts
		List<byte[]> workerReqs = _cluster.getAssignmentRequests("CrowdHammerWorker");
		// parse for number of available clients, distribute fairly
	}
	
	
	
}
