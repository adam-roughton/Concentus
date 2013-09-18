package com.adamroughton.concentus.crowdhammer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.javatuples.Pair;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.model.CollectiveApplication;

public final class Test {
	
	private final String _name;
	private final List<Pair<ServiceDeployment<ServiceState>, Integer>> _serviceDeployments;
	private final int _requiredNodeCount;
	private final int _workerCount;
	private final int[] _clientCounts;
	private final long _testDurationMillis;
	private final InstanceFactory<? extends ClientAgent> _clientAgentFactory;
	private final InstanceFactory<? extends CollectiveApplication> _applicationFactory;
	private final ComponentResolver<? extends ResizingBuffer> _componentResolver;
	
	Test(String name,
			List<Pair<ServiceDeployment<ServiceState>, Integer>> serviceDeployments, 
			int workerCount, 
			int[] clientCounts,
			long testDuration,
			TimeUnit unit,
			InstanceFactory<? extends ClientAgent> clientAgentFactory,
			InstanceFactory<? extends CollectiveApplication> applicationFactory,
			ComponentResolver<? extends ResizingBuffer> componentResolver) {
		_name = name;
		_serviceDeployments = serviceDeployments;
		int nodeCount = workerCount;
		for (Pair<ServiceDeployment<ServiceState>, Integer> deploymentCountPair : serviceDeployments) {
			 nodeCount += deploymentCountPair.getValue1();
		}
		_requiredNodeCount = nodeCount;
		_workerCount = workerCount;
		_clientCounts = clientCounts;
		_testDurationMillis = unit.toMillis(testDuration);
		_clientAgentFactory = clientAgentFactory;
		_applicationFactory = applicationFactory;
		_componentResolver = componentResolver;
	}
	
	
	public String getName() {
		return _name;
	}
	
	public InstanceFactory<? extends ClientAgent> getClientAgentFactory() {
		return _clientAgentFactory;
	}
	
	public InstanceFactory<? extends CollectiveApplication> getApplicationFactory() {
		return _applicationFactory;
	}

	public int getRequiredNodeCount() {
		return _requiredNodeCount;
	}

	public int getWorkerCount() {
		return _workerCount;
	}
	
	public int[] getClientCounts() {
		return _clientCounts;
	}

	public ComponentResolver<? extends ResizingBuffer> getComponentResolver() {
		return _componentResolver;
	}

	public long getTestDurationMillis() {
		return _testDurationMillis;
	}
	
	public List<Pair<ServiceDeployment<ServiceState>, Integer>> getServiceDeployments() {
		return new ArrayList<>(_serviceDeployments);
	}
	
}
