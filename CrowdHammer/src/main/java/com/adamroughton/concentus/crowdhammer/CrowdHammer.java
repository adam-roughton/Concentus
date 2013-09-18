package com.adamroughton.concentus.crowdhammer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.cluster.coordinator.GuardianManager;
import com.adamroughton.concentus.crowdhammer.TestTask.State;
import com.adamroughton.concentus.crowdhammer.TestTask.TestTaskEvent;
import com.adamroughton.concentus.crowdhammer.TestTask.TestTaskListener;
import com.adamroughton.concentus.crowdhammer.metriccollector.MetricCollector;
import com.adamroughton.concentus.data.ArrayBackedResizingBuffer;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.disruptor.StandardEventQueueFactory;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketManagerImpl;
import com.adamroughton.concentus.util.IdentityWrapper;
import com.esotericsoftware.minlog.Log;
import com.netflix.curator.framework.listen.Listenable;

public final class CrowdHammer implements Closeable {
	
	private static CrowdHammer INSTANCE;
	
	public static void runTest(Test testRun) {
		INSTANCE.startTest(testRun);
	}
	
	static <TBuffer extends ResizingBuffer> CrowdHammer createInstance(CoordinatorClusterHandle clusterHandle, 
			ConcentusHandle concentusHandle, Path resultDir) {
		if (INSTANCE != null) {
			throw new RuntimeException("Only one instance of CrowdHammer permitted per classloader.");
		}
		CrowdHammer instance = new CrowdHammer(clusterHandle, concentusHandle, resultDir);
		INSTANCE = instance;
		return instance;
	}
	
	private final CoordinatorClusterHandle _clusterHandle;
	private final ConcentusHandle _concentusHandle;
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final MetricCollector<ArrayBackedResizingBuffer> _metricCollector;
	
	private final GuardianManager _guardianManager;
	private TestTask _currentTask;
	
	public static class CrowdHammerEvent {
		public enum EventType {
			TEST_EVENT
		}
		
		private final EventType _eventType;
		private final TestTask _currentTestTask;
		private final TestTaskEvent _testTaskEvent;
		
		public CrowdHammerEvent(EventType eventType, 
				TestTask currentTestTask,
				TestTaskEvent testTaskEvent) {
			_eventType = Objects.requireNonNull(eventType);
			_currentTestTask = currentTestTask;
			_testTaskEvent = testTaskEvent;
		}
		
		public EventType getEventType() {
			return _eventType;
		}
		
		public String getCurrentTestName() {
			if (_currentTestTask != null) {
				return _currentTestTask.getTestName();
			} else {
				return null;
			}
		}
		
		public TestTask.State getTestState() {
			if (_testTaskEvent == null) {
				return null;
			} else {
				return _testTaskEvent.getState();
			}
		}
		
		public Exception getException() {
			return _testTaskEvent == null? null : _testTaskEvent.getException();
		}
		
		public boolean hadException() {
			return _testTaskEvent == null? false : _testTaskEvent.hadException();
		}
		
	}
	
	public interface CrowdHammerListener {
		void onCrowdHammerEvent(CrowdHammer crowdHammer, CrowdHammerEvent event);
	}
	
	private final Executor _defaultListenerExecutor = Executors.newSingleThreadExecutor();
	private final ConcurrentMap<IdentityWrapper<CrowdHammerListener>, Executor> _listeners = new ConcurrentHashMap<>();
	private Listenable<CrowdHammerListener> _listenable = new Listenable<CrowdHammerListener>() {
		
		@Override
		public void removeListener(CrowdHammerListener listener) {
			_listeners.remove(new IdentityWrapper<>(listener));
		}
		
		@Override
		public void addListener(CrowdHammerListener listener, Executor executor) {
			Objects.requireNonNull(listener);
			if (executor == null) {
				executor = _defaultListenerExecutor;
			}
			_listeners.put(new IdentityWrapper<>(listener), executor);
		}
		
		@Override
		public void addListener(CrowdHammerListener listener) {
			addListener(listener, null);
		}
	};
	
	private final TestTaskListener _testListener = new TestTaskListener() {
		
		@Override
		public void onTestTaskEvent(final TestTask task, final TestTaskEvent event) {
			for (Entry<IdentityWrapper<CrowdHammerListener>, Executor> entry : _listeners.entrySet()) {
				final CrowdHammerListener listener = entry.getKey().get();
				Executor executor = entry.getValue();
				executor.execute(new Runnable() {

					@Override
					public void run() {
						listener.onCrowdHammerEvent(CrowdHammer.this, new CrowdHammerEvent(
								CrowdHammerEvent.EventType.TEST_EVENT, task, event));
					}
					
				});
			}
		}
	};
	
	private CrowdHammer(CoordinatorClusterHandle clusterHandle, ConcentusHandle concentusHandle, Path resultDir) {
		_clusterHandle = Objects.requireNonNull(clusterHandle);
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		int metricCollectorPort = 9090;
		_metricCollector = new MetricCollector<>("CrowdHammer" + System.currentTimeMillis(), 
				resultDir, clusterHandle, concentusHandle, new ComponentResolver<ArrayBackedResizingBuffer>() {

					@Override
					public SocketManager<ArrayBackedResizingBuffer> newSocketManager(Clock clock) {
						return new SocketManagerImpl(clock);
					}

					@Override
					public EventQueueFactory getEventQueueFactory() {
						return new StandardEventQueueFactory();
					}
				}, metricCollectorPort, 2048);
		_guardianManager = new GuardianManager(_clusterHandle, _metricCollector);
	}
	
	public void start() throws Exception {
		_metricCollector.start();
		_guardianManager.start();
	}
	
	public void close() throws IOException {
		try {
			stopAnyCurrentTest();
		} catch (InterruptedException e) {
		}
		try {
			Log.info("Closing metric collector");
			_metricCollector.close();
		} catch (IOException e) {
		}
		try {
			Log.info("Closing guardian manager");
			_guardianManager.close();
		} catch (IOException e) {
		}
	}
	
	public Listenable<CrowdHammerListener> getListenable() {
		return _listenable;
	}
	
	public void startTest(Test test) {
		if (_currentTask != null && _currentTask.getState() != TestTask.State.STOPPED) {
			throw new RuntimeException("The previous test task should be stopped before a new task is started.");
		}
		_currentTask = new ClusterTestTask(test, _concentusHandle.getClock(), _clusterHandle, _guardianManager, _metricCollector);
		_currentTask.getListenable().addListener(_testListener);
		_executor.execute(_currentTask);
	}
	
	public void stopRun() throws InterruptedException {
		stopAnyCurrentTest();
	}
	
	private void stopAnyCurrentTest() throws InterruptedException {
		if (_currentTask != null) {
			Log.info("Stopping current test run");
			System.out.println();
			_currentTask.stop();
			try {
				_currentTask.waitForState(State.STOPPED, 10, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				throw new RuntimeException("Timed out waiting for current test to stop.", e);
			}
			_currentTask.getListenable().removeListener(_testListener);
			_currentTask = null;
		}
	}
	
}
