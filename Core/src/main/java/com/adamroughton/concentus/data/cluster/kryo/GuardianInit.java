package com.adamroughton.concentus.data.cluster.kryo;

import java.util.Objects;

import com.adamroughton.concentus.cluster.worker.ServiceDeployment;

public final class GuardianInit {

	private String[] _cmdLineArgs;
	private ServiceDeployment<ServiceState> _deployment;
	
	// for kryo
	@SuppressWarnings("unused")
	private GuardianInit() {	}
	
	public GuardianInit(String[] cmdLineArgs, ServiceDeployment<ServiceState> deployment) {
		_cmdLineArgs = Objects.requireNonNull(cmdLineArgs);
		_deployment = Objects.requireNonNull(deployment);
	}
	
	public String[] cmdLineArgs() {
		return _cmdLineArgs;
	}
	
	public ServiceDeployment<ServiceState> deployment() {
		return _deployment;
	}
	
}
