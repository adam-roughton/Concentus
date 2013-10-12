package com.adamroughton.concentus.crowdhammer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.javatuples.Pair;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.cluster.ClusterUtil;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.cluster.coordinator.GuardianManager;
import com.adamroughton.concentus.cluster.coordinator.GuardianManager.GuardianDeploymentListener;
import com.adamroughton.concentus.cluster.coordinator.GuardianManager.GuardianDeploymentState;
import com.adamroughton.concentus.cluster.coordinator.ServiceDependencySet;
import com.adamroughton.concentus.cluster.coordinator.GuardianManager.GuardianDeployment;
import com.adamroughton.concentus.cluster.coordinator.ServiceGroup;
import com.adamroughton.concentus.cluster.coordinator.ServiceGroup.ServiceGroupEvent;
import com.adamroughton.concentus.cluster.coordinator.ServiceGroup.ServiceGroupEvent.EventType;
import com.adamroughton.concentus.cluster.coordinator.ServiceGroup.ServiceGroupListener;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle;
import com.adamroughton.concentus.cluster.worker.Guardian;
import com.adamroughton.concentus.cluster.worker.ServiceDeployment;
import com.adamroughton.concentus.crowdhammer.metriccollector.MetricCollector;
import com.adamroughton.concentus.crowdhammer.worker.WorkerService;
import com.adamroughton.concentus.data.cluster.kryo.ProcessReturnInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceDeploymentPackage;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.metric.MetricBucketInfo;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.util.IdentityWrapper;
import com.adamroughton.concentus.util.TimeoutTracker;
import com.esotericsoftware.minlog.Log;
import com.netflix.curator.framework.listen.Listenable;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.CorePath.*;
import static com.adamroughton.concentus.data.cluster.kryo.ServiceState.*;

public class ClusterTestTask implements TestTask {

	private final Executor _backgroundExecutor = Executors.newSingleThreadExecutor();
	
	private final Test _test;
	private final Clock _testClock;
	private final CoordinatorClusterHandle _clusterHandle;
	private final GuardianManager _guardianManager;
	private final MetricCollector<?> _metricCollector;
	
	private final AtomicReference<State> _state = new AtomicReference<>(TestTask.State.CREATED);
	private final AtomicBoolean _stopFlag = new AtomicBoolean(false);
	
	private final ConcurrentMap<String, ServiceGroup<ServiceState>> _groupsByType = new ConcurrentHashMap<>();
	private final ConcurrentMap<IdentityWrapper<ServiceGroup<ServiceState>>, ServiceState> _groupStateLookup = new ConcurrentHashMap<>();
	private final List<GuardianDeployment<ServiceState>> _guardianDeployments = new ArrayList<>();
	
	private final Lock _backgroundUpdateLock = new ReentrantLock();
	private final Condition _backgroundUpdateCondition = _backgroundUpdateLock.newCondition();
	
	private final Lock _testStateLock = new ReentrantLock();
	private final Condition _testStateCondition = _testStateLock.newCondition();
	
	private final AtomicReference<Exception> _exceptionRef = new AtomicReference<>();
	private volatile TestRunInfo _currentRunInfo;
	
	private final Executor _defaultListenerExecutor = Executors.newSingleThreadExecutor();
	private final ConcurrentMap<IdentityWrapper<TestTaskListener>, Executor> _listeners = new ConcurrentHashMap<>();
	private Listenable<TestTaskListener> _listenable = new Listenable<TestTaskListener>() {
		
		@Override
		public void removeListener(TestTaskListener listener) {
			_listeners.remove(new IdentityWrapper<>(listener));
		}
		
		@Override
		public void addListener(TestTaskListener listener, Executor executor) {
			Objects.requireNonNull(listener);
			if (executor == null) {
				executor = _defaultListenerExecutor;
			}
			_listeners.put(new IdentityWrapper<>(listener), executor);
		}
		
		@Override
		public void addListener(TestTaskListener listener) {
			addListener(listener, null);
		}
	};
	
	private final TestTaskListener _defaultListener = new TestTaskListener() {
		
		@Override
		public void onTestTaskEvent(TestTask task, TestTaskEvent event) {
			_testStateLock.lock();
			try {
				_testStateCondition.signalAll();
			} finally {
				_testStateLock.unlock();
			}
		}
	};
	{
		_listenable.addListener(_defaultListener);
	}
	
	private final GuardianDeploymentListener<ServiceState> _deploymentListener = new GuardianDeploymentListener<ServiceState>() {
		
		@Override
		public void onDeploymentChange(
				GuardianDeployment<ServiceState> guardianDeployment,
				GuardianDeploymentState newState,
				ProcessReturnInfo retInfo) {
			if (newState != GuardianDeploymentState.RUNNING) {
				ServiceInfo<ServiceState> serviceInfo = guardianDeployment.getDeployment().serviceInfo();
				stopWithException(new RuntimeException("The deployment " + serviceInfo.serviceType() + 
						" on guardian " + guardianDeployment.getGuardianPath() + 
							" entered deployment state " + newState + " with ret info: " + 
							((retInfo == null)? "null" : retInfo.toString())));
			}
		}
	};
	
	private final ServiceGroupListener<ServiceState> _serviceGroupListener = new ServiceGroupListener<ServiceState>() {

		@Override
		public void onServiceGroupEvent(
				ServiceGroup<ServiceState> serviceGroup,
				ServiceGroupEvent<ServiceState> event) {
			if (event.getType() == EventType.STATE_READY) {
				Log.info("ClusterTestTask.onServiceGroupEvent: " + event.toString());
				_groupStateLookup.put(new IdentityWrapper<>(serviceGroup), event.getState());
				signalBackgroundUpdate();
			} else if (event.getType() == EventType.TIMED_OUT) {
				stopWithException(new RuntimeException("The service group " + serviceGroup.toString() + 
						" timed out while setting the " +
						"state to " + event.getState()));
			} else if (event.getType() == EventType.UPDATE_FAILURE) {
				stopWithException(new RuntimeException("The service group " + serviceGroup.toString() + 
						" failed while setting the " +
						"state to " + event.getState()));
			} else if (event.getType() == EventType.SERVICE_DEATH) {
				stopWithException(new RuntimeException("The service group " + serviceGroup.toString() + 
						" had a service die: " + event.getService().getServicePath()));
			}
		}
	};
	
	public ClusterTestTask(Test test, 
			Clock testClock,
			CoordinatorClusterHandle clusterHandle, 
			GuardianManager guardianManager,
			MetricCollector<?> metricCollector) {
		_test = Objects.requireNonNull(test);
		_testClock = Objects.requireNonNull(testClock);
		_metricCollector = Objects.requireNonNull(metricCollector);
		_guardianManager = Objects.requireNonNull(guardianManager);
		_clusterHandle = Objects.requireNonNull(clusterHandle);
		
		_currentRunInfo = new TestRunInfo(_test.getName(), 
				_test.getDeploymentSet().getDeploymentName());
	}
	
	@Override
	public void stop() {
		_stopFlag.set(true);
	}

	@Override
	public State getState() {
		return _state.get();
	}

	@Override
	public void waitForState(State state) throws InterruptedException {
		boolean hasState;
		_testStateLock.lock();
		try {
			hasState = _state.get() == state;
			while (!hasState) {
				_testStateCondition.await();
				hasState = _state.get() == state;
			}
		} finally {
			_testStateLock.unlock();
		}
	}
	
	@Override
	public void waitForState(State state, long timeout, TimeUnit unit)
			throws InterruptedException, TimeoutException {
		boolean hasState;
		TimeoutTracker timeoutTracker = new TimeoutTracker(timeout, unit);
		_testStateLock.lock();
		try {
			hasState = _state.get() == state;
			while (!hasState && !timeoutTracker.hasTimedOut()) {
				_testStateCondition.await(timeoutTracker.getTimeout(), 
						timeoutTracker.getUnit());
				hasState = _state.get() == state;
			}
		} finally {
			_testStateLock.unlock();
		}
		if (!hasState) {
			throw new TimeoutException();
		}
	}

	@Override
	public String getTestName() {
		return _test.getName();
	}
	
	@Override
	public void run() {
		try {
			if (!compareAndSetTestState(TestTask.State.CREATED, TestTask.State.SETTING_UP)) {
				throw new IllegalStateException("The test task can only be run once");
			}

			reportProgress("Setting up for test runs...");
			assertRunning();
			cleanUp();
			CollectiveApplication application = _test.getApplicationFactory().newInstance();
			ClientAgent agent = _test.getClientAgentFactory().newInstance();
			
			// put application into ZooKeeper
			String applicationPath = _clusterHandle.resolvePathFromRoot(APPLICATION);
			_clusterHandle.createOrSetEphemeral(applicationPath, _test.getApplicationFactory());
			
			// prepare dependency info
			TestDeploymentSet deploymentSet = _test.getDeploymentSet();
			
			// generate dependency set and deployment info
			ServiceDependencySet dependencySet = new ServiceDependencySet();
			Map<String, Integer> serviceCountLookup = new HashMap<>();

			reportProgress("Deployment Plan:");
			for (ServiceDeploymentPackage<ServiceState> deploymentPackage : deploymentSet) {
				ServiceDeployment<ServiceState> deployment = deploymentPackage.deployment();
				int count = deploymentPackage.count();
				
				ServiceInfo<ServiceState> serviceInfo = deployment.serviceInfo();
				dependencySet.addDependencies(serviceInfo.serviceType(), serviceInfo.dependencies());
				for (ServiceInfo<ServiceState> hostedService : deployment.getHostedServicesInfo()) {
					// make implicit dependency explicit
					dependencySet.addDependencies(hostedService.serviceType(), serviceInfo.serviceType());
					
					// add hosted service dependencies
					dependencySet.addDependencies(hostedService.serviceType(), hostedService.dependencies());
					
					// there is a one-to-one mapping between hosts and hosted
					// services
					reportProgress("ServiceType: " + hostedService.serviceType() + ", count: " + count);
					serviceCountLookup.put(hostedService.serviceType(), count);
				}
				
				reportProgress("ServiceType: " + serviceInfo.serviceType() + ", count: " + count);
				serviceCountLookup.put(serviceInfo.serviceType(), count);
			}
			
			reportProgress("Starting test runs...");
			for (int clientCount : _test.getClientCountIterable()) {
				_currentRunInfo = _currentRunInfo.withClientCount(clientCount);
				assertRunning();
				reportProgress("Doing run for test '" + _test.getName() + ":" + 
						_test.getDeploymentSet().getDeploymentName() + "' with client count: " + clientCount);
				TimeoutTracker timeoutTracker = new TimeoutTracker(10, TimeUnit.MINUTES);
				
				_metricCollector.newTestRun(_test.getName(), 
						clientCount, 
						_test.getTestDurationMillis(), 
						application.getClass(), 
						deploymentSet.getDeploymentName(),
						agent.getClass(), 
						mapToPairSet(serviceCountLookup.entrySet()));
				
				reportProgress("Deploying services...");
				for (ServiceDeploymentPackage<ServiceState> deploymentPackage : deploymentSet) {
					ServiceDeployment<ServiceState> deployment = deploymentPackage.deployment();
					int count = deploymentPackage.count();
					reportProgress("Deploying: " + count + " instance" + (count == 1? "": "s") +" of " + deployment.serviceInfo().serviceType());
					for (int i = 0; i < deploymentPackage.count(); i++) {
						assertRunning();
						GuardianDeployment<ServiceState> guardianDeployment = _guardianManager.deploy(deployment,
								deploymentPackage.componentResolver(),
								timeoutTracker.getTimeout(), 
								timeoutTracker.getUnit());
						guardianDeployment.getListenable().addListener(_deploymentListener, _backgroundExecutor);
						// act like this listener was registered before any state updates were made
						guardianDeployment.getListenable().resetListenerVersion(-1, _deploymentListener);
						_guardianDeployments.add(guardianDeployment);
					}
				}
				
				// start the services
				reportProgress("Starting services...");
				for (String serviceType : dependencySet) {
					for (ServiceState state : new ServiceState[] { CREATED, INIT, BIND, CONNECT, START }) {
						ServiceGroup<ServiceState> group;
						if (state == CREATED) {
							group = createGroup(serviceType, serviceCountLookup.get(serviceType), 
									timeoutTracker.getTimeout(), timeoutTracker.getUnit());
							group.waitForStateInBackground(CREATED, timeoutTracker.getTimeout(), timeoutTracker.getUnit());
							
						} else {
							group = _groupsByType.get(serviceType);

							if (state == INIT) {
								Map<String, ? extends Object> serviceInitData;
								
								// we need to initialise the workers with the client count they will
								// be simulated
								if (serviceType.equals(WorkerService.SERVICE_INFO.serviceType())) {
									List<Pair<String, Integer>> maxClientCounts = new ArrayList<>();
									for (ServiceHandle<ServiceState> workerService : group) {
										int maxClientCount = workerService.getCurrentState().getStateData(Integer.class);
										maxClientCounts.add(new Pair<>(workerService.getServicePath(), maxClientCount));
									}
									serviceInitData = createWorkerAllocations(clientCount, maxClientCounts);
								} else {
									serviceInitData = Collections.emptyMap();
								}
								
								for (ServiceHandle<ServiceState> service : group) {
									String servicePath = service.getServicePath();
									Object initData = serviceInitData.get(servicePath);
									_clusterHandle.setServiceInitData(_metricCollector, servicePath, serviceType, initData);
								}
							}
							
							reportProgress("Setting state to " + state + " for services of type " + serviceType + "...");
							group.enterStateInBackground(state, timeoutTracker.getTimeout(), timeoutTracker.getUnit());
						}
						
						waitForGroupToEnterState(serviceType, group, state, 
								timeoutTracker.getTimeout(), timeoutTracker.getUnit());
					}
				}
				
				assertRunning();
				reportProgress("Starting test...");
				setTestState(TestTask.State.RUNNING_TEST);
				
				// get the bucketId 60 seconds into the future
				MetricBucketInfo metricBucketInfo = new MetricBucketInfo(_testClock, Constants.METRIC_TICK);
				long startBucketId = metricBucketInfo.getBucketIdForTime(_testClock.currentMillis() + TimeUnit.SECONDS.toMillis(60), TimeUnit.MILLISECONDS);
				int bucketCount = metricBucketInfo.getBucketCount(_test.getTestDurationMillis(), TimeUnit.MILLISECONDS);
				
				// start collecting
				_metricCollector.startCollectingFrom(startBucketId, bucketCount);
				
				// wait for test time plus 30 seconds to allow tardy metric buckets to arrive
				assertRunning();
				long sleepTime = Math.max(0, 
						metricBucketInfo.getBucketEndTime(startBucketId + bucketCount) + 
						TimeUnit.SECONDS.toMillis(30) - _testClock.currentMillis());
				reportProgress("Waiting " + TimeUnit.MILLISECONDS.toSeconds(sleepTime) + " seconds until terminating current run");
				Thread.sleep(sleepTime);
				
				reportProgress("Stopping test...");
				timeoutTracker = new TimeoutTracker(10, TimeUnit.MINUTES);
				setTestState(TestTask.State.TEARING_DOWN);
				
				// tear down services (shutdown)
				for (String serviceType : dependencySet.inReverse()) {
					reportProgress("Tearing down services of type " + serviceType);
					ServiceGroup<ServiceState> group = _groupsByType.get(serviceType);
					group.enterStateInBackground(SHUTDOWN, timeoutTracker.getTimeout(), timeoutTracker.getUnit());
					
					// wait for the group to enter the state
					ServiceState groupState = _groupStateLookup.get(new IdentityWrapper<>(group));
					while (groupState != SHUTDOWN) {
						waitForBackgroundSignal(timeoutTracker.getTimeout(), timeoutTracker.getUnit());
						groupState = _groupStateLookup.get(new IdentityWrapper<>(group));
					}
					
					// close all service handles in the group
					for (ServiceHandle<ServiceState> serviceHandle : group) {
						serviceHandle.close();
					}
					group.getListenable().removeListener(_serviceGroupListener);
					String serviceTypePath = ZKPaths.makePath(_clusterHandle.resolvePathFromRoot(SERVICES), serviceType);
					_clusterHandle.deleteChildren(serviceTypePath);
				}
				cleanUp();
				_exceptionRef.set(null);
			}
			_currentRunInfo = _currentRunInfo.withNoClients();
		} catch (InterruptedException eInterrupted) {
			
		} catch (Exception eTest) {
			_exceptionRef.compareAndSet(null, eTest);
		} finally {
			try {
				// stop guardian deployments (ready)
				reportProgress("Stopping guardian deployments...");
				try {
					cleanUp();
				} catch (Exception e) {
					Log.error("Error cleaning up", e);
				}
				
				// remove application from ZooKeeper
				String applicationPath = _clusterHandle.resolvePathFromRoot(APPLICATION);
				_clusterHandle.delete(applicationPath);
			} finally {
				setTestState(TestTask.State.STOPPED);
				reportProgress("Finished test");
			}
		}
	}
	
	private ServiceGroup<ServiceState> createGroup(String serviceType, int count, long timeout, TimeUnit unit) throws Exception {
		String serviceTypePath = ZKPaths.makePath(_clusterHandle.resolvePathFromRoot(SERVICES), serviceType);
		reportProgress("Waiting for " + count + " instance" + (count == 1? "": "s") +" of " + serviceType + " to register...");
		List<String> serviceIds = ClusterUtil.waitForChildren(_clusterHandle.getClient(), serviceTypePath, count, 
				timeout, unit);
		List<String> servicePaths = ClusterUtil.toPaths(serviceTypePath, serviceIds);
		
		Log.info("Creating service group for " + serviceType + "...");
		ServiceGroup<ServiceState> serviceGroup = new ServiceGroup<>();
		serviceGroup.getListenable().addListener(_serviceGroupListener, _backgroundExecutor);
		for (String path : servicePaths) {
			ServiceHandle<ServiceState> serviceHandle = new ServiceHandle<>(path, 
					serviceType, ServiceState.class, _clusterHandle);
			serviceHandle.start();
			serviceGroup.addService(serviceHandle);
		}
		_groupsByType.put(serviceType, serviceGroup);
		return serviceGroup;
	}
	
	private void stopWithException(Exception exception) {
		_exceptionRef.compareAndSet(null, exception);
		signalBackgroundUpdate();
	}
	
	private void signalBackgroundUpdate() {
		_backgroundUpdateLock.lock();
		try {
			_backgroundUpdateCondition.signalAll();
		} finally {
			_backgroundUpdateLock.unlock();
		}
	}
	
	private void waitForBackgroundSignal(long time, TimeUnit unit) throws Exception {
		assertRunning();
		_backgroundUpdateLock.lock();
		try {
			if (!_backgroundUpdateCondition.await(time, unit)) {
				throw new TimeoutException();
			}
		} finally {
			_backgroundUpdateLock.unlock();
		}
		// ensure we are still running after the background signal
		assertRunning();
	}
	
	private void waitForGroupToEnterState(String serviceType, ServiceGroup<ServiceState> group, 
			ServiceState state, long timeout, TimeUnit unit) throws Exception {
		reportProgress("Waiting for services of type " + serviceType + " to enter state " + state + "...");
		// wait for the group to enter the state
		ServiceState groupState = _groupStateLookup.get(new IdentityWrapper<>(group));
		while (groupState != state) {
			waitForBackgroundSignal(timeout, unit);
			groupState = _groupStateLookup.get(new IdentityWrapper<>(group));
		}
	}
	
	@Override
	public Listenable<TestTaskListener> getListenable() {
		return _listenable;
	}
	
	private void assertRunning() throws Exception {
		if (_stopFlag.get()) throw new InterruptedException("Test stopped.");
		if (Thread.interrupted()) throw new InterruptedException("Test thread interrupted.");
		Exception exception = _exceptionRef.getAndSet(null);
		if (exception != null) {
			throw exception;
		}
	}
	
	private void cleanUp() throws Exception {	
		for (GuardianDeployment<ServiceState> deployment : _guardianDeployments) {
			deployment.getListenable().removeListener(_deploymentListener);
			deployment.stop();
		}
		_guardianDeployments.clear();
		_groupStateLookup.clear();
		_groupsByType.clear();
		_clusterHandle.deleteChildren(_clusterHandle.resolvePathFromRoot(SERVICE_ENDPOINTS));
		
		// delete all services that are not guardian services
		List<String> serviceTypes = _clusterHandle.getClient()
				.getChildren()
				.forPath(_clusterHandle.resolvePathFromRoot(SERVICES));
		for (String serviceType : serviceTypes) {
			if (!serviceType.equals(Guardian.SERVICE_TYPE)) {
				String serviceTypePath = ZKPaths.makePath(_clusterHandle.resolvePathFromRoot(SERVICES), serviceType);
				_clusterHandle.deleteChildren(serviceTypePath);
				_clusterHandle.delete(serviceTypePath);
			}
		}
	}
	
	private void setTestState(TestTask.State newState) {
		compareAndSetTestState(null, newState);
	}
	
	private boolean compareAndSetTestState(TestTask.State expectedState, final TestTask.State newState) {
		boolean result;
		if (expectedState != null) {
			result = _state.compareAndSet(expectedState, newState);
		} else {			
			_state.set(newState);			
			result = true;
		}
		sendToListeners(new TestTaskEvent(_currentRunInfo, newState, _exceptionRef.get()));
		return result;
	}
	
	private void reportProgress(String message) {
		sendToListeners(new TestTaskEvent(_currentRunInfo, message));
		Log.info(message);
	}
	
	private void sendToListeners(final TestTaskEvent testTaskEvent) {
		for (Entry<IdentityWrapper<TestTaskListener>, Executor> entry : _listeners.entrySet()) {
			final TestTaskListener listener = entry.getKey().get();
			Executor executor = entry.getValue();
			executor.execute(new Runnable() {

				@Override
				public void run() {
					listener.onTestTaskEvent(ClusterTestTask.this, testTaskEvent);
				}
				
			});
		}
	}
	
	private static Map<String, Integer> createWorkerAllocations(int testClientCount, List<Pair<String, Integer>> workerMaxClientCounts) {
		int workerCount = workerMaxClientCounts.size();
		List<Pair<String, Integer>> workerAllocations = new ArrayList<>(workerCount);
		int[] workerMaxClientCountLookup = new int[workerCount];
		double[] targetWorkerRatioLookup = new double[workerCount];
		double maxWorkerRatio = 0;
		int globalMaxClients = 0;
		
		for (int i = 0; i < workerCount; i++) {
			String workerPath = workerMaxClientCounts.get(i).getValue0();
			int maxClientCount = workerMaxClientCounts.get(i).getValue1();
			workerMaxClientCountLookup[i] = maxClientCount;
			globalMaxClients += maxClientCount;
			workerAllocations.add(i, new Pair<>(workerPath, 0));
		}
		
		if (testClientCount > globalMaxClients)
			throw new RuntimeException(String.format("The total available client count %d is less than " +
					"the requested test client count %d.", globalMaxClients, testClientCount));
		
		for (int i = 0; i < workerCount; i++) {
			double workerClientRatio = (double) workerMaxClientCountLookup[i] / (double) globalMaxClients;
			targetWorkerRatioLookup[i] = workerClientRatio;
			if (workerClientRatio > maxWorkerRatio) maxWorkerRatio = workerClientRatio;
		}
		
		Pair<String, Integer> workerAllocation;
		int remainingCount = testClientCount;
		while (remainingCount * maxWorkerRatio >= 1) {
			/* we will be able to allocate at least one on this run by applying
			 * the target ratios, so continue
			 */
			long runAllocationCount = remainingCount;
			long amountAllocatedOnRun = 0;
			for (int i = 0; i < workerCount; i++) {
				workerAllocation = workerAllocations.get(i);
				int allocationChange = (int) (runAllocationCount * targetWorkerRatioLookup[i]);
				workerAllocations.set(i, new Pair<>(workerAllocation.getValue0(), workerAllocation.getValue1() + allocationChange));
				amountAllocatedOnRun += allocationChange;
			}
			remainingCount -= amountAllocatedOnRun;
		}
		
		int currentWorkerIndex = 0;
		while (remainingCount > 0) {
			/* all target ratios will give 0 for the remaining amount,
			 * so resort to adding one at a time up to the maximum
			 * amount for each worker
			 */
			workerAllocation = workerAllocations.get(currentWorkerIndex);
			if (workerAllocation.getValue1() + 1 <= workerMaxClientCountLookup[currentWorkerIndex]) {
				workerAllocations.set(currentWorkerIndex, new Pair<>(workerAllocation.getValue0(), workerAllocation.getValue1() + 1));
				remainingCount--;
			}
			currentWorkerIndex = (currentWorkerIndex + 1) % workerCount;
		}

		Map<String, Integer> allocationsMap = new HashMap<>(workerAllocations.size());
		for (Pair<String, Integer> allocation : workerAllocations) {
			allocationsMap.put(allocation.getValue0(), allocation.getValue1());
		}
		return allocationsMap;
	}

	private static Set<Pair<String, Integer>> mapToPairSet(Set<Entry<String, Integer>> entrySet) {
		HashSet<Pair<String, Integer>> pairSet = new HashSet<>(entrySet.size());
		for (Entry<String, Integer> entry : entrySet) {
			pairSet.add(new Pair<>(entry.getKey(), entry.getValue()));
		}
		return pairSet;
	}

}
