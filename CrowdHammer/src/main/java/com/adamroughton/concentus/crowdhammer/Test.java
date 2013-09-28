package com.adamroughton.concentus.crowdhammer;

import it.unimi.dsi.fastutil.ints.IntIterable;

import java.util.concurrent.TimeUnit;

import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.model.CollectiveApplication;

public final class Test {
	
	private final String _name;
	private final TestDeploymentSet _deploymentSet;
	private final IntIterable _clientCountIterable;
	private final long _testDurationMillis;
	private final InstanceFactory<? extends CollectiveApplication> _applicationFactory;
	
	public Test(String name,
			InstanceFactory<? extends CollectiveApplication> applicationFactory,
			TestDeploymentSet deploymentSet, 
			IntIterable clientCountIterable,
			long testDuration,
			TimeUnit unit) {
		_name = name;
		_deploymentSet = deploymentSet;
		_clientCountIterable = clientCountIterable;
		_testDurationMillis = unit.toMillis(testDuration);
		_applicationFactory = applicationFactory;
	}

	public String getName() {
		return _name;
	}
	
	public InstanceFactory<? extends CollectiveApplication> getApplicationFactory() {
		return _applicationFactory;
	}
	
	public InstanceFactory<? extends ClientAgent> getClientAgentFactory() {
		return _deploymentSet.getClientAgentFactory();
	}

	public TestDeploymentSet getDeploymentSet() {
		return _deploymentSet;
	}
	
	public IntIterable getClientCountIterable() {
		return _clientCountIterable;
	}

	public long getTestDurationMillis() {
		return _testDurationMillis;
	}
	
}
