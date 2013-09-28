package com.adamroughton.concentus.crowdhammer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.adamroughton.concentus.ArrayBackedComponentResolver;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.crowdhammer.worker.WorkerService;
import com.adamroughton.concentus.crowdhammer.worker.WorkerService.WorkerServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceDeploymentPackage;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;

public class TestDeploymentSet implements Iterable<ServiceDeploymentPackage<ServiceState>> {

	private static final String WORKER_TYPE = WorkerService.SERVICE_INFO.serviceType();
	
	private String _deploymentName;
	private InstanceFactory<? extends ClientAgent> _clientAgentFactory;
	private Map<String, ServiceDeploymentPackage<ServiceState>> _deploymentMap = new HashMap<>();
	private ComponentResolver<? extends ResizingBuffer> _defaultResolver;
	
	private int _requiredGuardianCountCache = -1;
	
	// for Kryo
	@SuppressWarnings("unused")
	private TestDeploymentSet() {}
	
	public TestDeploymentSet(String deploymentName, InstanceFactory<? extends ClientAgent> clientAgentFactory) {
		this(deploymentName, clientAgentFactory, new ArrayBackedComponentResolver());
	}
	
	public TestDeploymentSet(String deploymentName, InstanceFactory<? extends ClientAgent> clientAgentFactory, 
			ComponentResolver<? extends ResizingBuffer> defaultComponentResolver) {
		_deploymentName = Objects.requireNonNull(deploymentName);
		_clientAgentFactory = Objects.requireNonNull(clientAgentFactory);
		_defaultResolver = Objects.requireNonNull(defaultComponentResolver);
		addDefaultWorkerDeployment(1);
	}
	
	public String getDeploymentName() {
		return _deploymentName;
	}
	
	public TestDeploymentSet addDeployment(ServiceDeployment<ServiceState> serviceDeployment, int count) {
		return addDeployment(serviceDeployment, count, _defaultResolver);
	}
	
	public TestDeploymentSet addDeployment(ServiceDeployment<ServiceState> serviceDeployment, int count,
			ComponentResolver<? extends ResizingBuffer> componentResolver) {
		_deploymentMap.put(serviceDeployment.serviceInfo().serviceType(), 
				new ServiceDeploymentPackage<>(serviceDeployment, count, componentResolver));
		clearCachedValues();
		return this;
	}
	
	public ServiceDeploymentPackage<ServiceState> getDeploymentPackage(String serviceType) {
		return _deploymentMap.get(serviceType);
	}
	
	public TestDeploymentSet setWorkerCount(int workerCount) {
		if (!_deploymentMap.containsKey(WORKER_TYPE)) {
			addDefaultWorkerDeployment(workerCount);
		} else {
			ServiceDeploymentPackage<ServiceState> workerDeploymentPackage = _deploymentMap.get(WORKER_TYPE);
			_deploymentMap.put(WORKER_TYPE, new ServiceDeploymentPackage<>(workerDeploymentPackage.deployment(), 
					workerCount, workerDeploymentPackage.componentResolver()));	
		}
		clearCachedValues();
		return this;
	}
	
	public int getWorkerCount() {
		return _deploymentMap.get(WORKER_TYPE).count();
	}

	public List<ServiceDeploymentPackage<ServiceState>> getDeploymentPackages() {
		return new ArrayList<>(_deploymentMap.values());
	}
	
	public int getRequiredGuardianCount() {
		if (_requiredGuardianCountCache == -1) {
			int count = 0;
			for (ServiceDeploymentPackage<ServiceState> deploymentPackage : _deploymentMap.values()) {
				count += deploymentPackage.count();
			}
			_requiredGuardianCountCache = count;
		}
		return _requiredGuardianCountCache;
	}
	
	@Override
	public Iterator<ServiceDeploymentPackage<ServiceState>> iterator() {
		final Iterator<Entry<String, ServiceDeploymentPackage<ServiceState>>> mapIterator = 
				_deploymentMap.entrySet().iterator();
		return new Iterator<ServiceDeploymentPackage<ServiceState>>() {

			@Override
			public boolean hasNext() {
				return mapIterator.hasNext();
			}

			@Override
			public ServiceDeploymentPackage<ServiceState> next() {
				return mapIterator.next().getValue();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
	
	public InstanceFactory<? extends ClientAgent> getClientAgentFactory() {
		return _clientAgentFactory;
	}
	
	private void addDefaultWorkerDeployment(int workerCount) {
		ServiceDeployment<ServiceState> workerDeployment = new WorkerServiceDeployment(_clientAgentFactory, 32000, -1, 
				Constants.DEFAULT_MSG_BUFFER_SIZE, Constants.DEFAULT_MSG_BUFFER_SIZE);
		addDeployment(workerDeployment, workerCount);
	}
	
	private void clearCachedValues() {
		_requiredGuardianCountCache = -1;
	}

}
