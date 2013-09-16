package com.adamroughton.concentus.cluster.worker;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.metric.MetricContext;

public interface ServiceDeployment<TState extends Enum<TState> & ClusterState> {

	Class<TState> stateType();
	
	String serviceType();
	
	String[] serviceDependencies();
	
	void onPreStart(StateData<TState> stateData);
	
	<TBuffer extends ResizingBuffer> ClusterService<TState> createService(
			int serviceId,
			ServiceContext<TState> context,
			ConcentusHandle handle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver);
	
}
