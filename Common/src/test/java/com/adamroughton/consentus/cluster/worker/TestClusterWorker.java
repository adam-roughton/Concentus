package com.adamroughton.consentus.cluster.worker;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adamroughton.consentus.cluster.ClusterPath;
import com.adamroughton.consentus.cluster.ClusterState;
import com.adamroughton.consentus.cluster.ExceptionCallback;
import com.adamroughton.consentus.cluster.TestClusterBase;
import com.adamroughton.consentus.cluster.TestState1;

import static org.junit.Assert.*;

public class TestClusterWorker extends TestClusterBase {

	private ExecutorService _executor;
	private ExceptionCallback _exCallback;
	private ClusterListenerStateCapturer<TestState1> _stateChangeCapturer;
	private ClusterWorker _clusterWorker;
	
	@Before
	public void setUp() {
		_executor = Executors.newCachedThreadPool();
		_exCallback = new ExceptionCallback();
		_stateChangeCapturer = new ClusterListenerStateCapturer<>(TestState1.class);
		_clusterWorker = new ClusterWorker(getZooKeeperAddress(), ROOT, _stateChangeCapturer, _executor, _exCallback);
	}
	
	@After
	public void tearDown() throws Exception {
		_clusterWorker.close();
	}
	
	@Test
	public void initStateChangeMade() throws Exception {
		TestState1 initState = TestState1.ONE;
		ClusterState initStateData = new ClusterState(initState.domain(), initState.code());
		getTestClient().create()
			.creatingParentsIfNeeded()
			.forPath(ClusterPath.STATE.getPath(ROOT), ClusterState.toBytes(initStateData));
		
		_clusterWorker.start();
		Thread.sleep(500);
		List<TestState1> stateChanges = _stateChangeCapturer.getCapturedStates();
		assertEquals(1, stateChanges.size());
		assertEquals(TestState1.ONE, stateChanges.get(0));
	}
	
	@Test
	public void ljlChangeMade() throws Exception {
		TestState1 initState = TestState1.ONE;
		ClusterState initStateData = new ClusterState(initState.domain(), initState.code());
		getTestClient().create()
			.creatingParentsIfNeeded()
			.forPath(ClusterPath.STATE.getPath(ROOT), ClusterState.toBytes(initStateData));
		
		_clusterWorker.start();
		Thread.sleep(500);
		List<TestState1> stateChanges = _stateChangeCapturer.getCapturedStates();
		assertEquals(1, stateChanges.size());
		assertEquals(TestState1.ONE, stateChanges.get(0));
	}
	
}
