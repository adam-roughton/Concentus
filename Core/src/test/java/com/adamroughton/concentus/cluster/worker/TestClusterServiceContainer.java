package com.adamroughton.concentus.cluster.worker;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.javatuples.Quartet;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.DrivableClock;
import com.adamroughton.concentus.cluster.ClusterHandleSettings;
import com.adamroughton.concentus.cluster.ClusterUtil;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.ExceptionCallback;
import com.adamroughton.concentus.cluster.TestClusterBase;
import com.adamroughton.concentus.data.BufferFactory;
import com.adamroughton.concentus.data.NullResizingBuffer;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ClusterState;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInit;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.data.cluster.kryo.StateEntry;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.StubSocketManager;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.utils.ZKPaths;

import static org.junit.Assert.*;
import static com.adamroughton.concentus.cluster.HelperMethods.*;

public class TestClusterServiceContainer extends TestClusterBase {

	private static final UUID CLUSTER_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
	
	private ExceptionCallback _exCallback;
	private Kryo _kryo;
	
	@Before
	public void before() throws Exception {
		_exCallback = new ExceptionCallback();
		_kryo = Util.newKryoInstance();
	}
	
	private static class TestComponentResolver implements ComponentResolver<ResizingBuffer> {

		@Override
		public ZmqSocketManager<ResizingBuffer> newSocketManager(Clock clock) {
			return new StubSocketManager<>(new BufferFactory<ResizingBuffer>() {

				@Override
				public ResizingBuffer newInstance(int defaultBufferSize) {
					return new NullResizingBuffer();
				}

				@Override
				public ResizingBuffer[] newCollocatedSet(int defaultBufferSize,
						int count) {
					ResizingBuffer[] set = new ResizingBuffer[count];
					for (int i = 0; i < count; i++) {
						set[i] = new NullResizingBuffer();
					}
					return set;
				}
			});
		}

		@Override
		public EventQueueFactory getEventQueueFactory() {
			return new StandardEventQueueFactory();
		}
		
	}
	
	@Test
	public void registration() throws Throwable {
		Triplet<String, ServiceContainerImpl<ServiceState>, ClusterServiceCollector<ServiceState>> 
				containerTuple = newContainerWithCollector(CLUSTER_ID, "Test", null);
		String serviceRootPath = containerTuple.getValue0();
		
		String statePath = CorePath.SERVICE_STATE.getAbsolutePath(serviceRootPath);
		assertNotNull("State path not created", getTestClient().checkExists().forPath(statePath));
		
		String stateTypePath = CorePath.SERVICE_STATE_TYPE.getAbsolutePath(serviceRootPath);
		assertNotNull("State path not created", getTestClient().checkExists().forPath(stateTypePath));
		
		byte[] stateTypeBytes = getTestClient().getData().forPath(stateTypePath);
		Class<?> stateType = Util.fromKryoBytes(_kryo, stateTypeBytes, Class.class);
		assertEquals(ServiceState.class, stateType);
		
		StateEntry<ServiceState> stateEntry = getServiceState(getTestClient(), _kryo, serviceRootPath, ServiceState.class);
		assertNotNull("State entry was null", stateEntry);
		assertEquals(ServiceState.CREATED, stateEntry.getState());
	}
	
	@Test
	public void changeStateNoData() throws Throwable {
		Triplet<String, ServiceContainerImpl<ServiceState>, ClusterServiceCollector<ServiceState>> 
			containerTuple = newContainerWithCollector(CLUSTER_ID, "Test", null);
		String serviceRootPath = containerTuple.getValue0();
		ClusterServiceCollector<ServiceState> collector = containerTuple.getValue2();
		
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.INIT, null);
		waitForState(getTestClient(), _kryo, _exCallback, serviceRootPath, ServiceState.INIT, 5000, TimeUnit.MILLISECONDS);
		
		List<StateChange<ServiceState>> stateChanges = collector.valueCollector().getValues();
		assertEquals(1, stateChanges.size());
		assertEquals(ServiceState.INIT, stateChanges.get(0).newServiceState);
		assertNull(stateChanges.get(0).stateData);
	}
	
	@Test
	public void changeStateSignalData() throws Throwable {
		Triplet<String, ServiceContainerImpl<ServiceState>, ClusterServiceCollector<ServiceState>> 
			containerTuple = newContainerWithCollector(CLUSTER_ID, "Test", null);
		String serviceRootPath = containerTuple.getValue0();
		ClusterServiceCollector<ServiceState> collector = containerTuple.getValue2();
		
		long signalData = 5000;
		
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.INIT, signalData);
		waitForState(getTestClient(), _kryo, _exCallback, serviceRootPath, ServiceState.INIT, 5000, TimeUnit.MILLISECONDS);
		
		List<StateChange<ServiceState>> stateChanges = collector.valueCollector().getValues();
		assertEquals(1, stateChanges.size());
		assertEquals(ServiceState.INIT, stateChanges.get(0).newServiceState);
		assertEquals(signalData, ((Long)stateChanges.get(0).stateData).longValue());
	}
	
	@Test
	public void changeStateFromInternalEvent() throws Throwable {
		ClusterServiceCollector<ServiceState> clusterService = new ClusterServiceCollector<>();
		
		Quartet<String, ServiceContainerImpl<ServiceState>, ClusterServiceCollector<ServiceState>, AtomicReference<ServiceContext<ServiceState>>> 
			containerTuple = newContainer(CLUSTER_ID, clusterService, ServiceState.class, "test", null);
		String serviceRootPath = containerTuple.getValue0();
		ClusterServiceCollector<ServiceState> collector = containerTuple.getValue2();
		AtomicReference<ServiceContext<ServiceState>> serviceContextRef = containerTuple.getValue3();
		
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.INIT, null);
		waitForState(getTestClient(), _kryo, _exCallback, serviceRootPath, ServiceState.INIT, 5000, TimeUnit.MILLISECONDS);
		
		// get the service context
		ServiceContext<ServiceState> serviceContext = serviceContextRef.get();
		assertNotNull(serviceContext);
		
		// update the state internally
		serviceContext.enterState(ServiceState.SHUTDOWN, null, ServiceState.INIT);
		waitForState(getTestClient(), _kryo, _exCallback, serviceRootPath, ServiceState.SHUTDOWN, 5000, TimeUnit.MILLISECONDS);
		
		List<StateChange<ServiceState>> stateChanges = collector.valueCollector().getValues();
		assertEquals(2, stateChanges.size());
		assertEquals(ServiceState.INIT, stateChanges.get(0).newServiceState);
		assertEquals(ServiceState.SHUTDOWN, stateChanges.get(1).newServiceState);
	}
	
	@Test(timeout=10000)
	public void changeStateFromInternalEventDuringSignalChange() throws Throwable {
		final CyclicBarrier barrier = new CyclicBarrier(2);
		ClusterServiceCollector<ServiceState> clusterService = new ClusterServiceCollector<ServiceState>() {

			@Override
			public void onStateChanged(ServiceState newServiceState,
					int stateChangeIndex,StateData stateData, ClusterHandle cluster)
					throws Exception {
				barrier.await();
				super.onStateChanged(newServiceState, stateChangeIndex, stateData, cluster);
				barrier.await();
			}
			
		};
		
		Quartet<String, ServiceContainerImpl<ServiceState>, ClusterServiceCollector<ServiceState>, AtomicReference<ServiceContext<ServiceState>>> 
			containerTuple = newContainer(CLUSTER_ID, clusterService, ServiceState.class, "test", null);
		String serviceRootPath = containerTuple.getValue0();
		ClusterServiceCollector<ServiceState> collector = containerTuple.getValue2();
		AtomicReference<ServiceContext<ServiceState>> serviceContextRef = containerTuple.getValue3();
		
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.INIT, null);
		barrier.await();
		barrier.await();
		waitForState(getTestClient(), _kryo, _exCallback, serviceRootPath, ServiceState.INIT, 5000, TimeUnit.MILLISECONDS);
		
		// get the service context
		ServiceContext<ServiceState> serviceContext = serviceContextRef.get();
		assertNotNull(serviceContext);
		
		// interrupt this signal with change that expects the previous state (should be ignored)
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.BIND, null);
		barrier.await();
		serviceContext.enterState(ServiceState.START, null, ServiceState.INIT);
		barrier.await();
		
		// do another change to ensure the interrupt was ignored
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.SHUTDOWN, null);
		barrier.await();
		barrier.await();
		waitForState(getTestClient(), _kryo, _exCallback, serviceRootPath, ServiceState.SHUTDOWN, 5000, TimeUnit.MILLISECONDS);
		
		List<StateChange<ServiceState>> stateChanges = collector.valueCollector().getValues();
		assertEquals(3, stateChanges.size());
		assertEquals(ServiceState.INIT, stateChanges.get(0).newServiceState);
		assertEquals(ServiceState.BIND, stateChanges.get(1).newServiceState);
		assertEquals(ServiceState.SHUTDOWN, stateChanges.get(2).newServiceState);
	}
	
	@Test
	public void changeStateDataForCoordinator() throws Throwable {
		final String expectedStateData = "TestingTesting123";
		
		ClusterService<ServiceState> service = new ClusterService<ServiceState>() {

			@Override
			public void onStateChanged(ServiceState newServiceState, int stateChangeIndex,
					StateData stateData, ClusterHandle cluster)
					throws Exception {
				stateData.setDataForCoordinator(expectedStateData);
			}
			
		};
		
		Quartet<String, ServiceContainerImpl<ServiceState>, ClusterService<ServiceState>, AtomicReference<ServiceContext<ServiceState>>> 
			containerTuple = newContainer(CLUSTER_ID, service, ServiceState.class, "Test", null);
		String serviceRootPath = containerTuple.getValue0();
		
		// signal a change to INIT
		signalState(getTestClient(), _kryo, serviceRootPath, ServiceState.class, ServiceState.INIT, null);
		
		// wait for the state to be processed
		String actualStateData = waitForState(getTestClient(), _kryo, _exCallback, 
				serviceRootPath, ServiceState.INIT, String.class, 5000, TimeUnit.MILLISECONDS);
		
		assertEquals(expectedStateData, actualStateData);
	}
	
	/*
	 * 
	 * Service creation methods
	 * 
	 */
	
	private Triplet<String, ServiceContainerImpl<ServiceState>, ClusterServiceCollector<ServiceState>> 
				newContainerWithCollector(UUID clusterId, String type, Object preStartData) 
			throws Throwable {
		ClusterServiceCollector<ServiceState> collector = new ClusterServiceCollector<>();
		return newContainer(clusterId, collector, ServiceState.class, "collector", preStartData).removeFrom3();
	}
	
	private <TState extends Enum<TState> & ClusterState, TService extends ClusterService<TState>> 
		Quartet<String, ServiceContainerImpl<TState>, TService, AtomicReference<ServiceContext<TState>>> 
			newContainer(UUID clusterId, final TService service, final Class<TState> stateType, final String serviceType, 
					final Object preStartData) throws Throwable {
		ConcentusHandle concentusHandle = new ConcentusHandle(new DrivableClock(), InetAddress.getLocalHost(), 
				getZooKeeperAddress(), Collections.<String>emptySet());
		ClusterHandleSettings clusterHandleSettings = new ClusterHandleSettings(getZooKeeperAddress(), ROOT, clusterId, _exCallback);
		
		final AtomicReference<ServiceContext<TState>> serviceContextContainer = new AtomicReference<>();
		ServiceDeployment<TState> deployment = new ServiceDeployment<TState>() {

			@Override
			public ServiceInfo<TState> serviceInfo() {
				return new ServiceInfo<>(serviceType, stateType);
			}
			
			@Override
			public Iterable<ServiceInfo<TState>> getHostedServicesInfo() {
				return Collections.emptyList();
			}

			@Override
			public <TBuffer extends ResizingBuffer> ClusterService<TState> createService(
					int serviceId,
					StateData initData,
					ServiceContext<TState> serviceContext,
					ConcentusHandle handle, MetricContext metricContext,
					ComponentResolver<TBuffer> resolver) {
				serviceContextContainer.set(serviceContext);
				return service;
			}

			@Override
			public void onPreStart(StateData stateData) {
				stateData.setDataForCoordinator(preStartData);
			}
			
		};
		
		ServiceContainerImpl<TState> container = new ServiceContainerImpl<>(clusterHandleSettings, concentusHandle, deployment, new TestComponentResolver());
		container.start();
		_exCallback.throwAnyExceptions();
		
		String serviceTypePath = ZKPaths.makePath(CorePath.SERVICES.getAbsolutePath(ROOT), serviceType);
		assertTrue("Service type path was not created", ClusterUtil.waitForExistence(getTestClient(), serviceTypePath, 1000, TimeUnit.MILLISECONDS));
		
		String serviceId = waitForChild(serviceTypePath, 1000, TimeUnit.MILLISECONDS);
		String serviceRootPath = ZKPaths.makePath(serviceTypePath, serviceId);
		
		// create init data
		ServiceInit serviceInitData = new ServiceInit(0, null);
		String serviceInitPath = CorePath.SERVICE_INIT_DATA.getAbsolutePath(serviceRootPath);
		getTestClient().create().creatingParentsIfNeeded().forPath(serviceInitPath, Util.toKryoBytes(_kryo, serviceInitData));
		
		// create fake metric endpoint
		ServiceEndpoint metricEndpoint = new ServiceEndpoint(-1, "metricCollector", "127.0.0.1", 8080);
		String metricEndpointPath = CorePath.METRIC_COLLECTOR.getAbsolutePath(ROOT);
		getTestClient().create().creatingParentsIfNeeded().forPath(metricEndpointPath, Util.toKryoBytes(_kryo, metricEndpoint));
		
		return new Quartet<>(serviceRootPath, container, service, serviceContextContainer);
	}
	
	private String waitForChild(final String path, long timeout, TimeUnit unit) throws Exception {
		return ClusterUtil.waitForChildren(getTestClient(), path, 1, timeout, unit).get(0);
	}
	
	/*
	 * Helper classes
	 * 
	 */
	
	private static class StateChange<TState> {
		public final TState newServiceState;
		public final Object stateData;
		public StateChange(TState newServiceState, Object stateData) {
			this.newServiceState = newServiceState;
			this.stateData = stateData;
		}
	}
	
	private static class ClusterServiceCollector<TState extends Enum<TState> & ClusterState> implements ClusterService<TState> {

		private ValueCollector<StateChange<TState>> _collector = new ValueCollector<>();
		
		@Override
		public void onStateChanged(TState newServiceState, int stateChangeIndex,
				StateData stateData, ClusterHandle cluster) throws Exception {
			StateChange<TState> change = new StateChange<>(newServiceState, stateData.getData(Object.class));
			_collector.addValue(change);
		}
		
		public ValueCollector<StateChange<TState>> valueCollector() {
			return _collector;
		}
		
	}
	
	
}
