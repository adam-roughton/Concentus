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
