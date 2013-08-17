package com.adamroughton.concentus;

import java.util.Map;

import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.messaging.ResizingBuffer;

public interface ConcentusCoordinatorNode<TConfig extends Configuration> extends ConcentusNode<TConfig> {

	<TBuffer extends ResizingBuffer> void run(Map<String, String> commandLineArgs, ConcentusHandle<TConfig, TBuffer> processHandle, ClusterCoordinatorHandle clusterHandle);
	
}
