package com.adamroughton.concentus.crowdhammer;

import java.util.Objects;
import java.util.logging.Logger;

import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes;
import com.adamroughton.concentus.cluster.data.TestRunInfo;

public class CrowdHammerCoordinator {

	private final static Logger LOG = Logger.getLogger("CrowdHammerCoordinator");
	
	private final ClusterCoordinatorHandle _clusterHandle;
	private ParticipatingNodes _participatingNodes = null;
	private int _nextRunId = 0;
	private boolean _hasInitializedTestInfrastructure = false;
	
	public CrowdHammerCoordinator(ClusterCoordinatorHandle clusterHandle) {
		_clusterHandle = Objects.requireNonNull(clusterHandle);
	}	
	
	public void ensureTestInfrastructureInitialised() throws InterruptedException {
		if (!_hasInitializedTestInfrastructure) {
			_participatingNodes = _clusterHandle.getNodeSnapshot();
			setAndWait(CrowdHammerServiceState.BIND);
			setAndWait(CrowdHammerServiceState.CONNECT);
			_hasInitializedTestInfrastructure = true;
		}
	}
	
	public void setAndWait(CrowdHammerServiceState newState) 
			throws InterruptedException {
		LOG.info(String.format("Setting state to %s", newState.name()));
		_clusterHandle.setState(newState);
		LOG.info("Waiting for nodes...");
		while (!_clusterHandle.waitForReady(_participatingNodes));
		LOG.info("All nodes ready, proceeding...");
	}
	
	public ParticipatingNodes getParticipatingNodes() {
		return _participatingNodes;
	}
	
	public ClusterCoordinatorHandle getClusterHandle() {
		return _clusterHandle;
	}
	
	public void newTestRun(int clientCount, long duration) {
		int runId = _nextRunId++;
		_clusterHandle.setRunInfo(new TestRunInfo(runId, clientCount, duration));
	}

}
