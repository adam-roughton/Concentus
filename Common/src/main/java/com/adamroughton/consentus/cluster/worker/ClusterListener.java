package com.adamroughton.consentus.cluster.worker;

public interface ClusterListener<S extends Enum<S> & ClusterStateValue> {

	/**
	 * Invoked when the state of the cluster changes.
	 * @param newClusterState
	 * @param cluster
	 * @throws Exception
	 */
	void onStateChanged(S newClusterState, Cluster cluster) throws Exception;
	
	Class<S> getStateValueClass();
	
}
