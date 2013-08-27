package com.adamroughton.concentus.cluster.worker;

import java.util.List;
import java.util.UUID;

import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.netflix.curator.framework.api.CuratorWatcher;

public interface ClusterWorkerHandle {
	
	String getApplicationClass();
	
	/**
	 * Registers the worker as a metric publisher with the generated {@link ClusterListenerHandle#getMyId() worker ID} for the cluster.
	 */
	void registerAsMetricPublisher(String type, String pubAddress, String metaDataReqAddress);
	
	List<MetricPublisherInfo> getMetricPublishers();
	
	List<MetricPublisherInfo> getMetricPublishers(CuratorWatcher watcher);
	
	void registerServiceEndpoint(ServiceEndpointType endpointTypeId, String hostAddress, int port);
	
	void registerService(String serviceType, String address);
	
	void unregisterServiceEndpoint(ServiceEndpointType endpointTypeId);
	
	void unregisterService(String serviceType);
	
	String getServiceAtRandom(String serviceType);
	
	String getServiceEndpointAtRandom(ServiceEndpointType endpointTypeId);
	
	String[] getAllServices(String serviceType);
	
	String[] getAllServiceEndpoints(ServiceEndpointType endpointTypeId);
	
	void registerAsActionProcessor(int id);
	
	void unregisterAsActionProcessor();
	
	List<Integer> getActionProcessorIds();
	
	UUID getMyId();

}
