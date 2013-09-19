package com.adamroughton.concentus.crowdhammer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.javatuples.Pair;

import com.adamroughton.concentus.ArrayBackedComponentResolver;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.InstanceFactory;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.model.CollectiveApplication;

public class TestBuilder {

	private String _testName;
	private final Map<String, Pair<ServiceDeployment<ServiceState>, Integer>> _deploymentMap = new HashMap<>();
	private int _workerCount = 1;
	private int[] _clientCounts = new int[] { 1000, 2000, 3000, 4000, 5000 };
	private long _testDuration = 2;
	private TimeUnit _unit = TimeUnit.MINUTES;
	private InstanceFactory<? extends ClientAgent> _agentFactory = null;
	private InstanceFactory<? extends CollectiveApplication> _applicationFactory = null;
	private ComponentResolver<? extends ResizingBuffer> _componentResolver = new ArrayBackedComponentResolver();
	
	public TestBuilder usingName(String testName) {
		_testName = testName;
		return this;
	}
	
	public TestBuilder withService(ServiceDeployment<ServiceState> serviceDeployment, int count) {
		_deploymentMap.put(serviceDeployment.serviceInfo().serviceType(), new Pair<>(serviceDeployment, count));
		return this;
	}
	
	public TestBuilder withWorkerCount(int workerCount) {
		_workerCount = workerCount;
		return this;
	}
	
	public TestBuilder withRunTime(long time, TimeUnit unit) {
		_testDuration = time;
		_unit = unit;
		return this;
	}
	
	public TestBuilder withAgentFactory(InstanceFactory<? extends ClientAgent> agentFactory) {
		_agentFactory = agentFactory;
		return this;
	}
	
	public TestBuilder withApplicationFactory(InstanceFactory<? extends CollectiveApplication> applicationFactory) {
		_applicationFactory = applicationFactory;
		return this;
	}
	
	public TestBuilder withComponentResolver(ComponentResolver<? extends ResizingBuffer> componentResolver) {
		_componentResolver = componentResolver;
		return this;
	}
	
	public TestBuilder withClientCounts(int... count) {
		_clientCounts = count;
		return this;
	}
	
	public Test build() {
		return new Test(_testName, 
				new ArrayList<>(_deploymentMap.values()),
				_workerCount, 
				_clientCounts,
				_testDuration,
				_unit,
				_agentFactory,
				_applicationFactory,
				_componentResolver);
	}
	
}
