package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

public final class ServiceInfo<TState extends Enum<TState> & ClusterState> {

	private String _serviceType;
	private Class<TState> _stateType;
	private String[] _dependencies;
	
	// for Kryo
	@SuppressWarnings("unused")
	private ServiceInfo() { }
	
	public ServiceInfo(String serviceType, Class<TState> stateType) {
		this(serviceType, stateType, new String[0]);
	}
	
	public ServiceInfo(String serviceType, Class<TState> stateType, ServiceInfo<?>...dependencies) {
		this(serviceType, stateType, toServiceTypes(dependencies));
	}
	
	public ServiceInfo(String serviceType, Class<TState> stateType, String...dependencies) {
		_serviceType = Objects.requireNonNull(serviceType);
		_stateType = Objects.requireNonNull(stateType);
		_dependencies = dependencies;
	}
	
	public String serviceType() {
		return _serviceType;
	}
	
	public Class<TState> stateType() {
		return _stateType;
	}
	
	public String[] dependencies() {
		return _dependencies;
	}
	
	private static <TState extends Enum<TState> & ClusterState> String[] toServiceTypes(ServiceInfo<?>... services) {
		String[] serviceTypes = new String[services.length];
		for (int i = 0; i < services.length; i++) {
			serviceTypes[i] = services[i].serviceType();
		}
		return serviceTypes;
	}
}
