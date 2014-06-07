package com.adamroughton.concentus.crowdhammer.metriccollector;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.javatuples.Pair;
import org.zeromq.ZMQ;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.cluster.CorePath;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.cluster.coordinator.MetricRegistrationCallback;
import com.adamroughton.concentus.cluster.coordinator.ServiceIdAllocator;
import com.adamroughton.concentus.crowdhammer.ClientAgent;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.KryoRegistratorDelegate;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.data.cluster.kryo.MetricSourceMetaData;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.events.bufferbacked.MetricEvent;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.InternalEventWriter;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventReader;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Util;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.minlog.Log;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.YieldingWaitStrategy;

public class MetricCollector<TBuffer extends ResizingBuffer> implements Closeable, ServiceIdAllocator {
	
	private final String _sessionName;
	private final Path _resultDir;
	private final int _metricPort;
	
	private final CoordinatorClusterHandle _clusterHandle;
	private final ConcentusHandle _concentusHandle;
	private final KryoRegistratorDelegate _kryoRegistrator;
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final ZmqSocketManager<TBuffer> _socketManager;
	
	private final EventQueue<TBuffer> _inputQueue;
	private final InternalEventWriter<TBuffer> _internalWriter;
	
	private MetricStore _metricStore;	
	private MetricRegistrationCallback _metricRegistrationListener;
	private IncomingEventHeader _header;
	private ProcessingPipeline<TBuffer> _pipeline;
	private EventListener<TBuffer> _eventListener;
	
	private int _currentTestRunId = -1;
	private int _nextMetricSourceId = 0;
	
	public MetricCollector(String sessionName, 
			Path resultDir,
			CoordinatorClusterHandle clusterHandle, 
			ConcentusHandle concentusHandle,
			ComponentResolver<TBuffer> componentResolver,
			int metricPort,
			int inputQueueLength) {
		_sessionName = Objects.requireNonNull(sessionName);
		_resultDir = Objects.requireNonNull(resultDir);
		_clusterHandle = Objects.requireNonNull(clusterHandle);
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_metricPort = metricPort;
		_socketManager = componentResolver.newSocketManager(concentusHandle.getClock());
		_header = new IncomingEventHeader(0, 1);
		
		EventQueueFactory eventQueueFactory = componentResolver.getEventQueueFactory();
		
		MessageQueueFactory<TBuffer> messageQueueFactory = 
				_socketManager.newMessageQueueFactory(eventQueueFactory);
		_inputQueue = messageQueueFactory.createMultiProducerQueue("input", inputQueueLength, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, new YieldingWaitStrategy());
		
		/*
		 * Register internal event types
		 */
		final Int2ObjectArrayMap<Class<?>> internalRegistrations = new Int2ObjectArrayMap<>();
		int nextFreeId = Util.newKryoInstance().getNextRegistrationId();
		internalRegistrations.put(nextFreeId++, NewTestRunEvent.class);
		internalRegistrations.put(nextFreeId++, CollectWindow.class);
		
		_kryoRegistrator = new KryoRegistratorDelegate() {
			
			@Override
			public void register(Kryo kryo) {
				for (int id : internalRegistrations.keySet()) {
					kryo.register(internalRegistrations.get(id), id);
				}
			}
		};
		_internalWriter = new InternalEventWriter<>("internal", _inputQueue, _header, _kryoRegistrator, true);
	}
	
	public void start() throws IOException {
		// create metric store
		_metricStore = createMetricStore(_sessionName, _resultDir);
		
		// bind to metric collecting port
		SocketSettings subSocketSettings = SocketSettings.create()
				.bindToPort(_metricPort)
				.subscribeTo(DataType.METRIC_EVENT);
		int metricSocketId = _socketManager.create(ZMQ.SUB, subSocketSettings, "sub");
		
		Mutex<Messenger<TBuffer>> subMessengerMutex = _socketManager.getSocketMutex(metricSocketId);
		_eventListener = new EventListener<TBuffer>("inputListener", _header, subMessengerMutex, _inputQueue, _concentusHandle);
		
		MetricEventHandler<TBuffer> metricEventHandler = new MetricEventHandler<>(
				_metricStore, 
				_kryoRegistrator, 
				_header, 
				_concentusHandle);
		
		// start event processor 
		_pipeline = ProcessingPipeline.<TBuffer>build(_eventListener, _concentusHandle.getClock())
				.thenConnector(_inputQueue)
				.then(_inputQueue.createEventProcessor("metricEventHandler", metricEventHandler))
				.createPipeline(_executor);
		_pipeline.start();
		
		// create ZK node listener for metrics
		_metricRegistrationListener = new MetricRegistrationCallback() {
			
			InternalEventWriter<TBuffer> _internalWriter = new InternalEventWriter<>("MetricRegistrationListener",
					_inputQueue, _header, _kryoRegistrator, true);
			
			@Override
			public void newMetric(MetricMetaData metricMetaData) {
				Log.info("Adding new metric " + metricMetaData);
				_internalWriter.writeEvent(metricMetaData);
			}
		};
		_clusterHandle.addMetricRegistrationListener(_metricRegistrationListener);
		
		// advertise collector endpoint
		String metricCollectorEndpointPath = _clusterHandle.resolvePathFromRoot(CorePath.METRIC_COLLECTOR);
		ServiceEndpoint metricCollectorEndpoint = new ServiceEndpoint(-1, "metricCollector", 
				_concentusHandle.getNetworkAddress().getHostAddress(), _metricPort);
		_clusterHandle.createOrSetEphemeral(metricCollectorEndpointPath, metricCollectorEndpoint);
	}
	
	public void close() throws IOException {
		_clusterHandle.removeMetricRegistrationListener(_metricRegistrationListener);
		try {
			_pipeline.halt(5000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
		_metricStore.close();
	}
	
	public void newTestRun(String testName, int clientCount, 
			long durationMillis, Class<? extends CollectiveApplication> applicationClass, String deploymentName,
			Class<? extends ClientAgent> agentClass, Set<Pair<String, Integer>> deploymentInfo) {
		_currentTestRunId++;
		
		sendInternal(new NewTestRunEvent(testName, _currentTestRunId, clientCount, durationMillis, 
				applicationClass, deploymentName, agentClass, deploymentInfo));
	}
	
	public int newMetricSource(String name, String serviceType) {
		int sourceId = _nextMetricSourceId++;
		sendInternal(new MetricSourceMetaData(sourceId, name, serviceType));
		return sourceId;
	}
	
	public void startCollectingFrom(long startBucketId, int bucketCount) {
		sendInternal(new CollectWindow(startBucketId, bucketCount));
	}
	
	private void sendInternal(Object event) {
		if (!_internalWriter.writeEvent(event)) {
			// shouldn't happen
			throw new RuntimeException("Failed to publish internal event to metric collector");
		}
	}
	
	public int getCurrentRunId() {
		return _currentTestRunId;
	}
	
	private static MetricStore createMetricStore(String sessionName, Path basePath) throws IOException {
		Path databasePath = null;
		int appendixNum = 0;
		do {
			try {
				Path attemptPath;
				if (appendixNum > 0) {
					attemptPath = basePath.resolve(sessionName + Integer.toString(appendixNum) + ".sqlite");
				} else {
					attemptPath = basePath.resolve(sessionName + ".sqlite");	
				}
				databasePath = Files.createFile(attemptPath);
			} catch (FileAlreadyExistsException eAlreadyExists) {
				appendixNum++;
			}
		} while (databasePath == null);
		
		return new SqliteMetricStore(databasePath);
	}
	
	private static class MetricEventHandler<TBuffer extends ResizingBuffer> implements EventHandler<TBuffer>, LifecycleAware {

		private final MetricStore _metricStore;
		private final Kryo _kryo;
		private final FatalExceptionCallback _exCallback;
		private final IncomingEventHeader _header;
		private int _runId;
		
		private CollectWindow _collectWindow = null;
		
		private final MetricEvent _metricEvent = new MetricEvent();
		
		public MetricEventHandler(MetricStore metricStore, 
				KryoRegistratorDelegate kryoRegistrator, 
				IncomingEventHeader header,
				FatalExceptionCallback exCallback) {
			_metricStore = metricStore;
			_kryo = Util.newKryoInstance();
			kryoRegistrator.register(_kryo);
			_header = header;
			_exCallback = exCallback;
			_runId = -1;
		}
		
		@Override
		public void onEvent(TBuffer event, long sequence, boolean endOfBatch)
				throws Exception {
			if (!EventHeader.isValid(event, 0))
				return;
			
			try {
				if (_header.isFromInternal(event)) {
					onInternalEvent(Util.fromKryoBytes(_kryo, event.slice(_header.getEventOffset()), Object.class));
				} else  {
					EventPattern.readContent(event, _header, _metricEvent, new EventReader<IncomingEventHeader, MetricEvent>() {

						@Override
						public void read(IncomingEventHeader header,
								MetricEvent event) {
							onMetricEvent(event);
						}
					});
				}
				if (endOfBatch) {
					_metricStore.onEndOfBatch();
				}
			} catch (Exception e) {
				_exCallback.signalFatalException(e);
			}
		}

		@Override
		public void onStart() {
		}

		@Override
		public void onShutdown() {
			_metricStore.onEndOfBatch();
			try {
				_metricStore.close();
			} catch (IOException eIO) {
				_exCallback.signalFatalException(eIO);
			}
		}
		
		private void onInternalEvent(Object internalEvent) {
			if (internalEvent instanceof NewTestRunEvent) {
				onNewTestRun((NewTestRunEvent) internalEvent);
			} else if (internalEvent instanceof MetricSourceMetaData) {
				onNewMetricSource((MetricSourceMetaData) internalEvent);
			} else if (internalEvent instanceof MetricMetaData) {
				onNewMetric((MetricMetaData) internalEvent);
			} else if (internalEvent instanceof CollectWindow) {
				onStartCollectingEvent((CollectWindow) internalEvent);
			} else if (internalEvent == null) {
				Log.warn("Null internal event");
			} else {
				Log.warn("Unknown internal event type " + internalEvent.getClass().getName());
			}
		}
		
		private void onNewTestRun(NewTestRunEvent event) {
			_runId = event.runId;
			_metricStore.pushTestRunMetaData(_runId, 
					event.testName,
					event.clientCount, 
					event.durationMillis, 
					event.applicationClass, 
					event.deploymentName,
					event.agentClass, 
					event.deploymentInfo);
		}

		private void onNewMetricSource(MetricSourceMetaData metricSourceData) {
			_metricStore.pushSourceMetaData(_runId, metricSourceData.getMetricSourceId(), 
					metricSourceData.getSourceName(), metricSourceData.getServiceType());
		}
		
		private void onNewMetric(MetricMetaData metricMetaData) {
			_metricStore.pushMetricMetaData(_runId, metricMetaData.getMetricSourceId(), 
					metricMetaData.getMetricId(), metricMetaData.getReference(), 
					metricMetaData.getMetricName(), metricMetaData.isCumulative());
		}
	
		private void onStartCollectingEvent(CollectWindow collectWindow) {
			_collectWindow = collectWindow;
		}
		
		private void onMetricEvent(MetricEvent metricEvent) {
			if (!shouldCollect(metricEvent)) return;
			
			int sourceId = metricEvent.getSourceId();
			int metricId = metricEvent.getMetricId();
			long bucketId = metricEvent.getMetricBucketId();
			long duration = metricEvent.getBucketDuration();
			
			ResizingBuffer valueSlice = metricEvent.getMetricValueSlice();
			
			switch (metricEvent.getMetricType()) {
				case COUNT:
					_metricStore.pushCountMetric(_runId, sourceId, metricId, 
							bucketId, duration, valueSlice.readLong(0));
					break;
				case THROUGHPUT:
					_metricStore.pushThroughputMetric(_runId, sourceId, metricId, 
							bucketId, duration, valueSlice.readLong(0));
					break;
				case STATS:
					_metricStore.pushStatsMetric(_runId, sourceId, metricId, 
							bucketId, duration, valueSlice.readRunningStats(0));
					break;
			}
		}
		
		private boolean shouldCollect(MetricEvent metricEvent) {
			if (_collectWindow == null) return false;
			
			long bucketId = metricEvent.getMetricBucketId();
			if (_collectWindow.startBucketId != -1 && bucketId >= _collectWindow.startBucketId) {
				if (_collectWindow.lastBucketId != -1) {
					return bucketId <= _collectWindow.lastBucketId;
				} else {
					return true;
				}
			} else {
				return false;
			}
		}
	}
	
	/*
	 * Internal Event Types
	 */
	
	private static class NewTestRunEvent {
		
		public String testName;
		public int runId;
		public int clientCount; 
		public long durationMillis; 
		public Class<? extends CollectiveApplication> applicationClass; 
		public String deploymentName;
		public Class<? extends ClientAgent> agentClass;
		public Set<Pair<String, Integer>> deploymentInfo;
		
		// for Kryo
		@SuppressWarnings("unused")
		private NewTestRunEvent() { }
		
		public NewTestRunEvent(String testName, int runId, int clientCount, 
			long durationMillis, Class<? extends CollectiveApplication> applicationClass, 
			String deploymentName, Class<? extends ClientAgent> agentClass, 
			Set<Pair<String, Integer>> deploymentInfo) {
			this.testName = testName;
			this.runId = runId;
			this.clientCount = clientCount;
			this.durationMillis = durationMillis;
			this.applicationClass = applicationClass;
			this.deploymentName = deploymentName;
			this.agentClass = agentClass;
			this.deploymentInfo = deploymentInfo;
		}
		
	}
	
	private static class CollectWindow {
		public long startBucketId;
		public long lastBucketId;
		
		// for Kryo
		@SuppressWarnings("unused")
		private CollectWindow() { }
		
		public CollectWindow(long startBucketId, int bucketCount) {
			this.startBucketId = startBucketId;
			this.lastBucketId = startBucketId + bucketCount;
		}
		
	}
	
}
