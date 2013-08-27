package com.adamroughton.concentus;

import java.util.Map;

import com.adamroughton.concentus.cluster.worker.ClusterListener;
import com.adamroughton.concentus.cluster.worker.ClusterStateValue;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.metric.MetricContext;

public interface ConcentusWorkerNode<TConfig extends Configuration,
		TClusterState extends Enum<TClusterState> & ClusterStateValue> extends ConcentusNode<TConfig> {

	<TBuffer extends ResizingBuffer> ClusterListener<TClusterState> createService(Map<String, String> commandLineOptions, 
			ConcentusHandle<? extends TConfig, TBuffer> handle, MetricContext metricContext);
	
	Class<TClusterState> getClusterStateClass();
	
}
