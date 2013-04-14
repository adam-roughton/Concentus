package com.adamroughton.concentus;

import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;

public interface ConcentusCoordinatorProcess {
	
	void run(ClusterCoordinatorHandle coordinatorHandle);
	
}
