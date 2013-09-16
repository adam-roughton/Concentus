package com.adamroughton.concentus.cluster.worker;

import java.util.Objects;

import com.adamroughton.concentus.data.cluster.kryo.ClusterState;

public abstract class ServiceDeploymentBase<TState extends Enum<TState> & ClusterState> implements ServiceDeployment<TState> {

	private String _serviceType;
	private Class<TState> _stateType;
	private String[] _dependencies;
	
	// for Kryo
	protected ServiceDeploymentBase() {
	}
	
	public ServiceDeploymentBase(String serviceType, Class<TState> stateType, String... dependencies) {
		_serviceType = Objects.requireNonNull(serviceType);
		_stateType = Objects.requireNonNull(stateType);
		_dependencies = Objects.requireNonNull(dependencies);
	}
	
	@Override
	public Class<TState> stateType() {
		return _stateType;
	}

	@Override
	public String serviceType() {
		return _serviceType;
	}

	@Override
	public String[] serviceDependencies() {
		return _dependencies;
	}

}
