package com.adamroughton.consentus.cluster.worker;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.KeeperException;

import com.adamroughton.consentus.FatalExceptionCallback;
import com.adamroughton.consentus.cluster.ClusterParticipant;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.dsl.Disruptor;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.consentus.cluster.ClusterPath.*;

public final class ClusterWorker extends ClusterParticipant implements Cluster, Closeable {
	
	private final List<String> _hostedServices;
	
	private final Disruptor<byte[]> _stateUpdateDisruptor;
	private final ClusterStateNodeListener _clusterStateNodeListener;
	private final ExecutorService _executor;
	
	@SuppressWarnings("unchecked")
	public <S extends Enum<S> & ClusterStateValue> ClusterWorker(
			final String zooKeeperAddress, 
			final String root, 
			final ClusterListener<S> clusterListener,
			final ExecutorService executor,
			final FatalExceptionCallback exHandler) {
		super(zooKeeperAddress, root, exHandler);
		_hostedServices = new ArrayList<>(1);
		_executor = Objects.requireNonNull(executor);
		
		_stateUpdateDisruptor = new Disruptor<>(new EventFactory<byte[]>() {
			public byte[] newInstance() {
				return new byte[8];
			}
		}, 16, _executor);
		
		_stateUpdateDisruptor.handleEventsWith(
				new ClusterStateEventHandler<S>(this, clusterListener, getExHandler()));
		
		_clusterStateNodeListener = new ClusterStateNodeListener(
				getClient(), 
				getPath(STATE), 
				_stateUpdateDisruptor, 
				getExHandler());
	}
	
	@Override
	public void start() throws Exception {
		super.start();
		_stateUpdateDisruptor.start();
		_clusterStateNodeListener.start();
	}
	
	@Override
	public void close() throws IOException {
		for (String serviceBasePath : _hostedServices) {
			delete(serviceBasePath);
		}
		super.close();
	}
	
	public void registerService(final String serviceType, final String address) {
		String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
		createServiceNode(serviceTypePath, address.getBytes());
		_hostedServices.add(serviceTypePath);
	}
	
	public String getServiceAtRandom(final String serviceType) {
		String service = null;
		try {
			String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
			ensurePathCreated(serviceTypePath);
			List<String> serviceNodes = getClient().getChildren().forPath(serviceTypePath);
			if (!serviceNodes.isEmpty()) {
				int index = (int) Math.round(Math.random() * (serviceNodes.size() - 1));
				String servicePath = serviceNodes.get(index);
				service = new String(getClient().getData().forPath(servicePath));
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return service;
	}
	
	public String[] getAllServices(final String serviceType) {
		String[] services = null;
		try {
			String serviceTypePath = ZKPaths.makePath(getPath(SERVICES), serviceType);
			ensurePathCreated(serviceTypePath);
			List<String> serviceNodes = getClient().getChildren().forPath(serviceTypePath);
			services = new String[serviceNodes.size()];
			for (int i = 0; i < serviceNodes.size(); i++) {
				services[i] = new String(getClient().getData().forPath(serviceNodes.get(i)));
			}
			
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return services;
	}
	
	public void requestAssignment(final String serviceType, final byte[] requestBytes) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
		createServiceNode(serviceTypeBase, requestBytes);
	}
	
	public byte[] getAssignment(final String serviceType) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_RES), serviceType);
		String serviceAssignmentPath = ZKPaths.makePath(serviceTypeBase, getMyIdString());
		byte[] assignment = null;
		try {
			ensurePathCreated(serviceTypeBase);
			assignment = getClient().getData().forPath(serviceAssignmentPath);
		} catch (KeeperException eKeeper) {
			if (eKeeper.code() == KeeperException.Code.NONODE) {
				return null;
			}
		} catch (Exception e) {
			getExHandler().signalFatalException(e);
		}
		return assignment;
	}
	
	public void deleteAssignmentRequest(final String serviceType) {
		String serviceTypeBase  = ZKPaths.makePath(getPath(ASSIGN_REQ), serviceType);
		deleteServiceNode(serviceTypeBase);
	}
	
	public void signalReady() {
		createServiceNode(getPath(READY), new byte[0]);
	}
	
	/**
	 * Creates an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 * @param data
	 */
	private void createServiceNode(final String parentPath, final byte[] data) {
		ensurePathCreated(parentPath);
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.createOrSetEphemeral(servicePath, data);
	}
	
	/**
	 * Deletes an ephemeral node with path equal to the parent path plus
	 * the service ID of this service.
	 * @param parentPath
	 */
	private void deleteServiceNode(final String parentPath) {
		String servicePath = ZKPaths.makePath(parentPath, getMyIdString());
		super.delete(servicePath);
	}
	
}
