package com.adamroughton.concentus.cluster.worker;

import static com.adamroughton.concentus.cluster.CorePath.*;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.cluster.ClusterPath;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.netflix.curator.utils.ZKPaths;

public class ClusterHandle extends ClusterParticipant implements Closeable {

	public ClusterHandle(String zooKeeperAddress, String root,
			UUID clusterId, FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, exHandler);
	}	
	
	public ClusterHandle(String zooKeeperAddress, String root,
			UUID clusterId, KryoRegistratorDelegate kryoRegistrator, FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, kryoRegistrator, exHandler);
	}	

	@SuppressWarnings("unchecked")
	public InstanceFactory<? extends CollectiveApplication> getApplicationInstanceFactory() {
		InstanceFactory<?> factoryObj = read(resolvePathFromRoot(APPLICATION), InstanceFactory.class);
		Class<?> instanceClass = factoryObj.instanceType();
		if (!instanceClass.isAssignableFrom(CollectiveApplication.class)) {
			throw new RuntimeException(String.format(
					"The InstanceFactory found does not create" +
					" instances of type CollectiveApplication (was %s)", 
					instanceClass.getName()));
		}
		return (InstanceFactory<? extends CollectiveApplication>) factoryObj;
	}

	public ServiceEndpoint getMetricCollectorEndpoint() {
		String metricCollectorPath = resolvePathFromRoot(METRIC_COLLECTOR);
		return read(metricCollectorPath, ServiceEndpoint.class);
	}
	
	public void registerMetric(MetricMetaData metricMetaData) {
		String metricsPath = resolvePathFromRoot(METRICS);
		
		// metric naming {sourceId_metricId}
		String newMetricPath = ZKPaths.makePath(metricsPath, 
				metricMetaData.getMetricSourceId() + "_" + metricMetaData.getMetricId());
		createOrSet(newMetricPath, metricMetaData);
	}
	
	public ServiceEndpoint getServiceEndpointAtRandom(String endpointType) {
		ServiceEndpoint endpoint = null;
		try {
			List<String> endpointPaths = getServiceEndpointPaths(endpointType);
			if (!endpointPaths.isEmpty()) {
				int index = (int) Math.round(Math.random() * (endpointPaths.size() - 1));
				endpoint = read(endpointPaths.get(index), ServiceEndpoint.class);
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return endpoint;
	}
	
	public List<ServiceEndpoint> getAllServiceEndpoints(String endpointType) {
		List<ServiceEndpoint> endpoints = new ArrayList<>();
		try {
			List<String> endpointPaths = getServiceEndpointPaths(endpointType);
			for (String endpointPath : endpointPaths) {
				endpoints.add(read(endpointPath, ServiceEndpoint.class));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return endpoints;
	}
	
	protected List<String> getServiceEndpointPaths(String endpointType) throws Exception {
		String endpointTypePath = endpointTypePath(endpointType);
		ensurePathCreated(endpointTypePath);
		List<String> paths = new ArrayList<>();
		for (String endpointId : getClient().getChildren().forPath(endpointTypePath)) {
			paths.add(ZKPaths.makePath(endpointTypePath, endpointId));
		}
		return paths;
	}
	
	protected String endpointTypePath(String endpointType) {
		return ZKPaths.makePath(resolvePathFromRoot(SERVICE_ENDPOINTS), endpointType);
	}

	public void unregisterServiceEndpoint(String endpointType) {
		delete(makeIdPath(endpointTypePath(endpointType)));
	}

	public void registerServiceEndpoint(ServiceEndpoint endpoint) {
		createOrSetEphemeral(makeIdPath(endpointTypePath(endpoint.type())), endpoint);
	}

	public String makeIdPath(ClusterPath basePathFromRoot) {
		return makeIdPath(resolvePathFromRoot(basePathFromRoot));
	}
	
	public String makeIdPath(String basePath) {
		return ZKPaths.makePath(basePath, getMyIdString());
	}

	
}
