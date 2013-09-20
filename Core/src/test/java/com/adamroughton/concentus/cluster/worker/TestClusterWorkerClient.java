package com.adamroughton.concentus.cluster.worker;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.adamroughton.concentus.cluster.ClusterHandleSettings;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.ExceptionCallback;
import com.adamroughton.concentus.cluster.TestClusterBase;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.netflix.curator.utils.ZKPaths;

import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestClusterWorkerClient extends TestClusterBase {

	private final static UUID PARTICIPANT_ID = UUID.fromString("abababab-abab-abab-abab-abababababab");
	
	private ExceptionCallback _exCallback;
	private ClusterHandle _workerClient;
	
	@Before
	public void before() throws Exception {
		_exCallback = new ExceptionCallback();
		_workerClient = new ClusterHandle(new ClusterHandleSettings(getZooKeeperAddress(), ROOT, PARTICIPANT_ID, _exCallback));
		_workerClient.start();
	}
	
	@After
	public void after() throws Exception {
		_workerClient.close();
	}
	
	@Test
	public void registerServiceEndpoint() throws Throwable {
		ServiceEndpoint endpoint = new ServiceEndpoint("test", "123.456.789.012", 1000);
		
		_workerClient.registerServiceEndpoint(endpoint);
		_exCallback.throwAnyExceptions();
		
		String endpointTypePath = ZKPaths.makePath(CorePath.SERVICE_ENDPOINTS.getAbsolutePath(ROOT), "test");
		String endpointPath = ZKPaths.makePath(endpointTypePath, _workerClient.getMyIdString());
		
		byte[] data = getTestClient().getData().forPath(endpointPath);
		assertNotNull(data);
		
		Kryo kryo = Util.newKryoInstance();
		Input input = new Input(data);
		Object obj = kryo.readClassAndObject(input);
		
		assertTrue("data was not a ServiceEndpoint", ServiceEndpoint.class.isAssignableFrom(obj.getClass()));
		assertEquals(endpoint, (ServiceEndpoint) obj);
	}
	
	@Test
	public void registerMultipleServiceEndpoints() throws Throwable {
		List<Pair<String, ServiceEndpoint>> expected = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			String type = "Test" + i;
			expected.add(new Pair<>(type, new ServiceEndpoint(type, String.format("123.456.789.%03d", i), 8080 + i)));
		}
		
		for (Pair<String, ServiceEndpoint> endpointTuple : expected) {
			_workerClient.registerServiceEndpoint(endpointTuple.getValue1());
			_exCallback.throwAnyExceptions();
		}
		
		for (Pair<String, ServiceEndpoint> endpointTuple : expected) {
			String endpointTypePath = ZKPaths.makePath(CorePath.SERVICE_ENDPOINTS.getAbsolutePath(ROOT), endpointTuple.getValue0());
			String endpointPath = ZKPaths.makePath(endpointTypePath, _workerClient.getMyIdString());
			
			byte[] data = getTestClient().getData().forPath(endpointPath);
			assertNotNull(data);
			
			Kryo kryo = Util.newKryoInstance();
			Input input = new Input(data);
			Object obj = kryo.readClassAndObject(input);
			
			assertTrue("data was not a ServiceEndpoint", ServiceEndpoint.class.isAssignableFrom(obj.getClass()));
			assertEquals(endpointTuple.getValue1(), (ServiceEndpoint) obj);
		}
	}
	
	@Test(expected=NullPointerException.class)
	public void registerServiceEndpointNull() throws Throwable {
		_workerClient.registerServiceEndpoint(null);
	}

	@Test
	public void getAllServiceEndpointsMultipleTypes() throws Throwable {
		String[] types = new String[] { "Type1", "Type2", "Type3" };
		
		ArrayList<Pair<UUID, ServiceEndpoint>> fakeEndpoints = new ArrayList<>();
		Map<String, List<ServiceEndpoint>> expectedByType = new HashMap<>();

		// generate fake end-points
		for (int typeId = 0; typeId < types.length; typeId++) {
			for (int serviceId = 0; serviceId < 10; serviceId++) {
				UUID fakeServiceId = new UUID(5678, serviceId);
				ServiceEndpoint endpoint = new ServiceEndpoint(types[typeId], String.format("123.456.789.%03d", serviceId), 8080 + typeId);
				fakeEndpoints.add(new Pair<>(fakeServiceId, endpoint));
				
				List<ServiceEndpoint> typeList;
				if (expectedByType.containsKey(types[typeId])) {
					typeList = expectedByType.get(types[typeId]);
				} else {
					typeList = new ArrayList<>();
					expectedByType.put(types[typeId], typeList);
				}
				typeList.add(endpoint);
			}
		}
		
		// put fake end-points into ZooKeeper
		insertEndpoints(fakeEndpoints);
		
		// test all end-points are found
		for (String type : types) {
			List<ServiceEndpoint> actual = _workerClient.getAllServiceEndpoints(type);
			_exCallback.throwAnyExceptions();
			
			List<ServiceEndpoint> expected = expectedByType.get(type);
			assertContentsEqual(expected, actual, endpointComparator());
		}
	}
	
	@Test
	public void getAllServiceEndpointsOneType() throws Throwable {
		ArrayList<Pair<UUID, ServiceEndpoint>> fakeEndpoints = new ArrayList<>();
		List<ServiceEndpoint> expected = new ArrayList<>();
		String type = "Type1";

		// generate fake end-points
		for (int serviceId = 0; serviceId < 10; serviceId++) {
			UUID fakeServiceId = new UUID(5678, serviceId);
			ServiceEndpoint endpoint = new ServiceEndpoint(type, String.format("123.456.789.%03d", serviceId), 8080);
			fakeEndpoints.add(new Pair<>(fakeServiceId, endpoint));
			expected.add(endpoint);
		}
		insertEndpoints(fakeEndpoints);
		
		// test all end-points are found
		List<ServiceEndpoint> actual = _workerClient.getAllServiceEndpoints(type);
		_exCallback.throwAnyExceptions();
		
		assertContentsEqual(expected, actual, endpointComparator());
	}
	
	@Test
	public void getAllServiceEndpointsNoEndpoints() throws Throwable {		
		List<ServiceEndpoint> actual = _workerClient.getAllServiceEndpoints("Test");
		_exCallback.throwAnyExceptions();
		assertEquals(0, actual.size());
	}
	
	private <T> void assertContentsEqual(List<T> expected, List<T> actual, Comparator<T> comparator) {
		assertEquals(expected.size(), actual.size());
		Collections.sort(actual, comparator);
		Collections.sort(expected, comparator);
		assertArrayEquals(actual.toArray(), expected.toArray());
	}
	
	private void insertEndpoints(Iterable<Pair<UUID, ServiceEndpoint>> endpoints) throws Exception {
		Kryo kryo = Util.newKryoInstance();
		
		// put fake end-points into ZooKeeper
		for (Pair<UUID, ServiceEndpoint> endpoint : endpoints) {
			ByteArrayOutputStream bOut = new ByteArrayOutputStream();
			Output output = new Output(bOut);
			kryo.writeClassAndObject(output, endpoint.getValue1());
			output.close();
			
			String endpointTypeRootPath = ZKPaths.makePath(CorePath.SERVICE_ENDPOINTS.getAbsolutePath(ROOT), endpoint.getValue1().type());
			String endpointPath = ZKPaths.makePath(endpointTypeRootPath, endpoint.getValue0().toString().replace("-", ""));
			
			getTestClient().create().creatingParentsIfNeeded().forPath(endpointPath, bOut.toByteArray());
		}
	}
	
	private Comparator<ServiceEndpoint> endpointComparator() {
		return new Comparator<ServiceEndpoint>() {

			@Override
			public int compare(ServiceEndpoint e1, ServiceEndpoint e2) {
				String[] e1IpParts = e1.ipAddress().split("\\.");
				String[] e2IpParts = e2.ipAddress().split("\\.");
				
				if (e1IpParts.length != 4 || e2IpParts.length != 4) {
					fail(String.format("Invalid IP address for test (e1IP = '%s', e2IP = '%s')", 
							e1.ipAddress(), e2.ipAddress()));
				}
				
				for (int i = 3; i >= 0; i--) {
					int e1Part = Integer.parseInt(e1IpParts[i]);
					int e2Part = Integer.parseInt(e2IpParts[i]);
					if (e1Part != e2Part) {
						return e2Part - e1Part;
					}
				}
				return e2.port() - e1.port();
			}
			
		};
	}
	
}
