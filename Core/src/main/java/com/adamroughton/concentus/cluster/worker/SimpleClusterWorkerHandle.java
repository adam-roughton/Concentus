package com.adamroughton.concentus.cluster.worker;

import static com.adamroughton.concentus.cluster.ClusterPath.METRIC_PUBLISHERS;
import static com.adamroughton.concentus.cluster.ClusterPath.SERVICES;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.zookeeper.WatchedEvent;

import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.ClusterParticipant;
import com.adamroughton.concentus.cluster.ClusterPath;
import com.adamroughton.concentus.cluster.data.MetricPublisherInfo;
import com.adamroughton.concentus.util.Util;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.utils.ZKPaths;

public class SimpleClusterWorkerHandle extends ClusterParticipant implements ClusterWorkerHandle, Closeable {

	private final Set<String> _hostedServices;

	public SimpleClusterWorkerHandle(String zooKeeperAddress, String root,
			UUID clusterId, FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, clusterId, exHandler);
		_hostedServices = new HashSet<>(1);
	}	
	
	
	@Override
	public void close() throws IOException {
		for (String serviceBasePath : _hostedServices) {
			delete(serviceBasePath);
		}
		super.close();
	}
	
	@Override
	public String getApplicationClass() {
		String applicationClassName = null;
		try {
			ensurePathCreated(getPath(ClusterPath.APPLICATION));
			applicationClassName = new String(getClient().getData().forPath(getPath(ClusterPath.APPLICATION)));
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return applicationClassName;
	}
	
	@Override
	public void registerAsMetricPublisher(String type, String pubAddress, String metaDataReqAddress) {
		UUID serviceId = getMyId();
		MetricPublisherInfo info = new MetricPublisherInfo(serviceId, type, pubAddress, metaDataReqAddress);	
		createServiceNode(getPath(METRIC_PUBLISHERS), info.getBytes());
	}
	
	@Override
	public List<MetricPublisherInfo> getMetricPublishers() {
		return getMetricPublishers(new CuratorWatcher() {

			@Override
			public void process(WatchedEvent event) throws Exception {
			}
			
		});
	}
	
	@Override
	public List<MetricPublisherInfo> getMetricPublishers(CuratorWatcher watcher) {
		List<MetricPublisherInfo> metricPublishers = null;
		try {
			ensurePathCreated(getPath(METRIC_PUBLISHERS));
			List<String> serviceIds = getClient().getChildren().usingWatcher(watcher).forPath(getPath(METRIC_PUBLISHERS));
			metricPublishers = new ArrayList<>(serviceIds.size());
			for (String serviceId : serviceIds) {
				String path = ZKPaths.makePath(getPath(METRIC_PUBLISHERS), serviceId);
				MetricPublisherInfo info = MetricPublisherInfo.fromBytes(getClient().getData().forPath(path));
				metricPublishers.add(info);
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return metricPublishers;
	}
	
	@Override
	public void registerServiceEndpoint(ServiceEndpointType endpointTypeId,
			String hostAddress, int port) {
		String endpointAddress = String.format("tcp://%s:%d", hostAddress, port);
		registerService(endpointTypeId.getId(), endpointAddress);
	}
	
	public void registerService(String serviceType, String address) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		createServiceNode(serviceTypePath, address.getBytes());
		_hostedServices.add(serviceTypePath);
	}
	
	public void unregisterService(String serviceType) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		deleteServiceNode(serviceTypePath);
		_hostedServices.remove(serviceTypePath);
	}
	
	@Override
	public void unregisterServiceEndpoint(ServiceEndpointType endpointTypeId) {
		unregisterService(endpointTypeId.getId());
	}

	@Override
	public String getServiceEndpointAtRandom(ServiceEndpointType endpointTypeId) {
		return getServiceAtRandom(endpointTypeId.getId());
	}

	@Override
	public String[] getAllServiceEndpoints(ServiceEndpointType endpointTypeId) {
		return getAllServices(endpointTypeId.getId());
	}
	
	public String getServiceAtRandom(String serviceType) {
		String service = null;
		try {
			List<String> serviceNodes = getAvailableServiceIds(serviceType);
			if (!serviceNodes.isEmpty()) {
				int index = (int) Math.round(Math.random() * (serviceNodes.size() - 1));
				service = getServiceAddress(serviceType, serviceNodes.get(index));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return service;
	}
	
	public String[] getAllServices(String serviceType) {
		String[] services = null;
		try {
			List<String> serviceNodes = getAvailableServiceIds(serviceType);
			services = new String[serviceNodes.size()];
			for (int i = 0; i < serviceNodes.size(); i++) {
				services[i] = getServiceAddress(serviceType, serviceNodes.get(i));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return services;
	}
	
	public void registerAsActionProcessor(int id) {
		byte[] idBytes = Util.intToBytes(id);
		createServiceNode(getPath(ClusterPath.ACTION_PROCESSOR_IDS), idBytes);
	}
	
	public void unregisterAsActionProcessor() {
		deleteServiceNode(getPath(ClusterPath.ACTION_PROCESSOR_IDS));
	}
	
	public List<Integer> getActionProcessorIds() {
		String path = getPath(ClusterPath.ACTION_PROCESSOR_IDS);
		ensurePathCreated(path);
		List<Integer> ids = new ArrayList<Integer>();
		try {
			List<String> actionProcessorServicePaths = getClient().getChildren().forPath(path);
			for (String actionProcessorServicePath : actionProcessorServicePaths) {
				byte[] idBytes = getClient().getData().forPath(actionProcessorServicePath);
				ids.add(Util.bytesToInt(idBytes));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return ids;
	}
	
	protected List<String> getAvailableServiceIds(String serviceType) throws Exception {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		ensurePathCreated(serviceTypePath);
		return getClient().getChildren().forPath(serviceTypePath);
	}
	
	protected String getServiceAddress(String serviceType, String serviceId) throws Exception {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		String servicePath = ZKPaths.makePath(serviceTypePath, serviceId);
		return new String(getClient().getData().forPath(servicePath));
	}
	
	/**
	 * Creates an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 * @param data
	 */
	protected void createServiceNode(String parentPath, byte[] data) {
		ensurePathCreated(parentPath);
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.createOrSetEphemeral(servicePath, data);
	}
	
	/**
	 * Deletes an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 */
	protected void deleteServiceNode(String parentPath) {
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.delete(servicePath);
	}
	
}
