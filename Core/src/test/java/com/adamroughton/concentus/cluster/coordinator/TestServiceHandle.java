package com.adamroughton.concentus.cluster.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.ZKUtil;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.ExceptionCallback;
import com.adamroughton.concentus.cluster.TestClusterBase;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.ServiceHandleEvent;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.ServiceHandleEvent.EventType;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.ServiceHandleListener;
import com.adamroughton.concentus.cluster.coordinator.ServiceHandle.StateEntry;
import com.adamroughton.concentus.cluster.worker.ValueCollector;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.netflix.curator.utils.ZKPaths;

import static com.adamroughton.concentus.cluster.HelperMethods.*;
import static org.junit.Assert.*;

public class TestServiceHandle extends TestClusterBase {

	private static final UUID HANDLE_CLUSTER_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
	private static final UUID TEST_SERVICE_ID = UUID.fromString("abababab-abab-abab-abab-abababababac");
	
	private final String _testServicePath = ZKPaths.makePath(CorePath.SERVICES.getAbsolutePath(ROOT), TEST_SERVICE_ID.toString());
	private ExceptionCallback _exCallback;
	private CoordinatorClusterHandle _clusterHandle;
	private ServiceHandle<ServiceState> _serviceHandle;
	private ValueCollector<Pair<String, ServiceHandleEvent<ServiceState>>> _listenerInvocationCollector;
	private Kryo _kryo;
	
	private ServiceHandleListener<ServiceState> _listener;
	
	@Before
	public void before() throws Exception {
		String serviceStatePath = CorePath.SERVICE_STATE.getAbsolutePath(_testServicePath);
		// if we don't use null here for the data, the IP address is inserted in causing
		// issues with reading the state
		getTestClient().create().creatingParentsIfNeeded().forPath(serviceStatePath, null);
		
		_listenerInvocationCollector = new ValueCollector<>();
		_listener = new ServiceHandleListener<ServiceState>() {

			@Override
			public void onServiceHandleEvent(
					ServiceHandle<ServiceState> serviceHandle,
					ServiceHandleEvent<ServiceState> event) {
				_listenerInvocationCollector.addValue(new Pair<>(serviceHandle.getServicePath(), event));
			}
			
		};
		
		_kryo = Util.newKryoInstance();
		
		_exCallback = new ExceptionCallback();
		_clusterHandle = new CoordinatorClusterHandle(getZooKeeperAddress(), ROOT, HANDLE_CLUSTER_ID, _exCallback);
		_clusterHandle.start();
		_serviceHandle = new ServiceHandle<>(_testServicePath, "test", ServiceState.class, _clusterHandle);
		_serviceHandle.getListenable().addListener(_listener);
		_serviceHandle.start();
	}
	
	@After
	public void after() throws Exception {
		_serviceHandle.close();
		_clusterHandle.close();
		ZKUtil.deleteRecursive(getTestClient().getZookeeperClient().getZooKeeper(), _testServicePath);
	}
	
	@Test
	public void stateChangeFromSignal() throws Throwable {
		StateEntry<ServiceState> initialState = _serviceHandle.getCurrentState();
		assertNull(initialState.getState());
		assertNull(initialState.getStateData(Object.class));
	
		// set signal data
		signalState(getTestClient(), _kryo, _testServicePath, ServiceState.INIT, null);
		
		// change the state (simulate service)
		setServiceState(getTestClient(), _kryo, _testServicePath, ServiceState.INIT, null);
		
		// wait for update
		_listenerInvocationCollector.waitForCount(1, 5000, TimeUnit.MILLISECONDS);
		List<Pair<String, ServiceHandleEvent<ServiceState>>> values = _listenerInvocationCollector.getValues();
		assertEquals(1, values.size());
		
		ServiceHandleEvent<ServiceState> expected = new ServiceHandleEvent<ServiceState>(
				EventType.STATE_CHANGED, ServiceState.INIT, ServiceState.INIT, null, null);
		ServiceHandleEvent<ServiceState> actual = values.get(0).getValue1();
		assertEventMatches(expected, actual);
	}
	
	@Test
	public void stateChangeNoSignal() throws Throwable {
		StateEntry<ServiceState> initialState = _serviceHandle.getCurrentState();
		assertNull(initialState.getState());
		assertNull(initialState.getStateData(Object.class));
		
		// change the state
		setServiceState(getTestClient(), _kryo, _testServicePath, ServiceState.INIT, null);
		
		// wait for update
		_listenerInvocationCollector.waitForCount(1, 5000, TimeUnit.MILLISECONDS);
		List<Pair<String, ServiceHandleEvent<ServiceState>>> values = _listenerInvocationCollector.getValues();
		assertEquals(1, values.size());
		
		ServiceHandleEvent<ServiceState> expected = new ServiceHandleEvent<ServiceState>(
				EventType.STATE_CHANGED, ServiceState.INIT, null, null, null);
		ServiceHandleEvent<ServiceState> actual = values.get(0).getValue1();
		assertEventMatches(expected, actual);
	}
	
	@Test
	public void stateChangeNoSignalWithData() throws Throwable {
		StateEntry<ServiceState> initialState = _serviceHandle.getCurrentState();
		assertNull(initialState.getState());
		assertNull(initialState.getStateData(Object.class));
		
		// set signal data
		String signalData = "SignalData";
		signalState(getTestClient(), _kryo, _testServicePath, ServiceState.INIT, signalData);
		
		// change the state
		String stateData = "StateData";
		setServiceState(getTestClient(), _kryo, _testServicePath, ServiceState.INIT, stateData);
		
		// wait for update
		_listenerInvocationCollector.waitForCount(1, 5000, TimeUnit.MILLISECONDS);
		List<Pair<String, ServiceHandleEvent<ServiceState>>> values = _listenerInvocationCollector.getValues();
		assertEquals(1, values.size());
		
		ServiceHandleEvent<ServiceState> expected = new ServiceHandleEvent<ServiceState>(
				EventType.STATE_CHANGED, ServiceState.INIT, ServiceState.INIT, stateData, signalData);
		ServiceHandleEvent<ServiceState> actual = values.get(0).getValue1();
		assertEventMatches(expected, actual);
	}
	
	@Test
	public void multipleStateChanges() throws Throwable {
		StateEntry<ServiceState> initialState = _serviceHandle.getCurrentState();
		assertNull(initialState.getState());
		assertNull(initialState.getStateData(Object.class));
		
		List<ServiceHandleEvent<ServiceState>> expectedList = new ArrayList<>();
		int count = 0;
		for (ServiceState state : new ServiceState[] { ServiceState.INIT, ServiceState.BIND, 
				ServiceState.CONNECT, ServiceState.START}) {
			// set signal data
			String signalData = "SignalData" + state;
			signalState(getTestClient(), _kryo, _testServicePath, state, signalData);
			
			// change the state
			String stateData = "StateData" + state;
			setServiceState(getTestClient(), _kryo, _testServicePath, state, stateData);
			
			expectedList.add(new ServiceHandleEvent<ServiceState>(
					EventType.STATE_CHANGED, state, state, stateData, signalData));
			
			// wait for update
			_listenerInvocationCollector.waitForCount(++count, 5000, TimeUnit.MILLISECONDS);
		}
		
		List<Pair<String, ServiceHandleEvent<ServiceState>>> values = _listenerInvocationCollector.getValues();
		assertEquals(count, values.size());
		
		for (int i = 0; i < count; i++) {
			ServiceHandleEvent<ServiceState> actual = values.get(i).getValue1();
			assertEventMatches(expectedList.get(i), actual);
		}
	}
	
	private void assertEventMatches(ServiceHandleEvent<ServiceState> expected, ServiceHandleEvent<ServiceState> actual) {
		if (expected == null && actual == null) return;
		if (expected == null || actual == null) fail("Expected was " + (expected == null? "null" : "not null")  + 
				" and actual was " + (actual == null? "null" : "not null"));
		
		assertEquals(expected.getEventType(), actual.getEventType());
		assertEquals(expected.getCurrentState(), actual.getCurrentState());
		assertEquals(expected.getStateData(Object.class), actual.getStateData(String.class));
		assertEquals(expected.getLastSignal(), actual.getLastSignal());
		assertEquals(expected.getLastSignalData(Object.class), actual.getLastSignalData(Object.class));
	}
	
}
