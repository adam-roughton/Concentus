package com.adamroughton.concentus;

import java.util.Map;

import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.config.Configuration;

public interface ConcentusCoordinatorNode<TConfig extends Configuration> extends ConcentusNode<TConfig> {

	void run(Map<String, String> commandLineArgs, ConcentusHandle<TConfig> processHandle, ClusterCoordinatorHandle clusterHandle);
	
}
