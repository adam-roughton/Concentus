package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;

public class ServiceDeploymentPackage<TState extends Enum<TState> & ClusterState> {

	private ServiceDeployment<TState> _deployment;
	private int _count;
	private ComponentResolver<? extends ResizingBuffer> _componentResolver;
	
	// for Kryo
	@SuppressWarnings("unused")
	private ServiceDeploymentPackage() { }
	
	public ServiceDeploymentPackage(ServiceDeployment<TState> deployment, 
			ComponentResolver<? extends ResizingBuffer> componentResolver) {
		this(deployment, 1, componentResolver);
	}
	
	public ServiceDeploymentPackage(ServiceDeployment<TState> deployment, 
			int count, 
			ComponentResolver<? extends ResizingBuffer> componentResolver) {
		_deployment = Objects.requireNonNull(deployment);
		if (count < 1) {
			throw new IllegalArgumentException("Must be at least one instance of " +
					"the deployment (told to deploy " + count + " instance(s))");
		}
		_count = count;
		_componentResolver = Objects.requireNonNull(componentResolver);
	}
	
	public ServiceDeployment<TState> deployment() {
		return _deployment;
	}
	
	public int count() {
		return _count;
	}
	
	public ComponentResolver<? extends ResizingBuffer> componentResolver() {
		return _componentResolver;
	}

}
