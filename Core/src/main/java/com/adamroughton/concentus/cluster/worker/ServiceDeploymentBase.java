package com.adamroughton.concentus.cluster.worker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;

public abstract class ServiceDeploymentBase<TState extends Enum<TState> & ClusterState> implements ServiceDeployment<TState> {

	private ServiceInfo<TState> _serviceInfo;
	private List<ServiceInfo<TState>> _hostedServiceInfo;
	
	// for Kryo
	protected ServiceDeploymentBase() {
	}
	
	@SafeVarargs
	public ServiceDeploymentBase(ServiceInfo<TState> serviceInfo, ServiceInfo<TState>... hostedServicesInfo) {
		_serviceInfo = Objects.requireNonNull(serviceInfo);
		
		_hostedServiceInfo = new ArrayList<>(hostedServicesInfo.length);
		HashSet<String> serviceTypeSet = new HashSet<>(hostedServicesInfo.length);
		for (ServiceInfo<TState> hostedServiceInfo : hostedServicesInfo) {
			if (!hostedServiceInfo.stateType().equals(serviceInfo.stateType())) {
				throw new IllegalArgumentException("The hosted services must have the same state type as the " +
						"primary service (expected " + serviceInfo.stateType().getCanonicalName() + ", got " + 
						hostedServiceInfo.stateType().getCanonicalName() + ")");
			}
			if (serviceTypeSet.contains(hostedServiceInfo.serviceType())) {
				throw new IllegalArgumentException("The hosted service " + hostedServiceInfo.serviceType() + 
						" has already been defined");
			}
			serviceTypeSet.add(hostedServiceInfo.serviceType());
			_hostedServiceInfo.add(hostedServiceInfo);
		}
	}

	@Override
	public ServiceInfo<TState> serviceInfo() {
		return _serviceInfo;
	}

	@Override
	public Iterable<ServiceInfo<TState>> getHostedServicesInfo() {
		return _hostedServiceInfo;
	}

}
