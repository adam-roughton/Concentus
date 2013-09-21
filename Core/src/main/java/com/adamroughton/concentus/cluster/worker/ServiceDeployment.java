package com.adamroughton.concentus.cluster.worker;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.metric.MetricContext;

public interface ServiceDeployment<TState extends Enum<TState> & ClusterState> {

	/**
	 * Gets info on the service to be deployed (i.e. the service created
	 * by {@link ServiceDeployment#createService(int, ServiceContext, ConcentusHandle, MetricContext, ComponentResolver)}).
	 * @return info on the service to deploy
	 */
	ServiceInfo<TState> serviceInfo();
	
	/**
	 * If this deployment hosts other services, provide info on them so that
	 * they can participate in the cluster.
	 * @return a set containing info on services that this service will host
	 */
	Iterable<ServiceInfo<TState>> getHostedServicesInfo();
	
	void onPreStart(StateData stateData);
	
	<TBuffer extends ResizingBuffer> ClusterService<TState> createService(
			int serviceId,
			StateData initData,
			ServiceContext<TState> context,
			ConcentusHandle handle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver);
	
}
