package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;

public final class GuardianInit {

	private String[] _cmdLineArgs;
	private ServiceDeployment<ServiceState> _deployment;
	private ComponentResolver<? extends ResizingBuffer> _componentResolver;
	
	// for kryo
	@SuppressWarnings("unused")
	private GuardianInit() {	}
	
	public GuardianInit(String[] cmdLineArgs, 
			ServiceDeployment<ServiceState> deployment,
			ComponentResolver<? extends ResizingBuffer> componentResolver) {
		_cmdLineArgs = Objects.requireNonNull(cmdLineArgs);
		_deployment = Objects.requireNonNull(deployment);
		_componentResolver = Objects.requireNonNull(componentResolver);
	}
	
	public String[] cmdLineArgs() {
		return _cmdLineArgs;
	}
	
	public ServiceDeployment<ServiceState> deployment() {
		return _deployment;
	}
	
	public ComponentResolver<? extends ResizingBuffer> componentResolver() {
		return _componentResolver;
	}
	
}
