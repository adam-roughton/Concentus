package com.adamroughton.concentus.canonicalstate;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.CoreServices;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.cluster.worker.ClusterService;
import com.adamroughton.concentus.cluster.worker.ConcentusServiceBase;
import com.adamroughton.concentus.cluster.worker.ServiceContext;
import com.adamroughton.concentus.cluster.worker.ServiceDeploymentBase;
import com.adamroughton.concentus.cluster.worker.StateData;
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.lmax.disruptor.BlockingWaitStrategy;

public class TickTimerService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase {

	public static final ServiceInfo<ServiceState> SERVICE_INFO = new ServiceInfo<>(
			CoreServices.TICK_TIMER.getId(), 
			ServiceState.class, 
			CoreServices.CANONICAL_STATE.getId(),
			CoreServices.ACTION_PROCESSOR.getId());

	public static class TickTimerServiceDeployment extends ServiceDeploymentBase<ServiceState> {
	
		private long _tickDuration;
		private long _simStartTime;
		
		// for Kryo
		@SuppressWarnings("unused")
		private TickTimerServiceDeployment() {}
		
		public TickTimerServiceDeployment(long tickDuration, long simStartTime) {
			super(SERVICE_INFO);
			_tickDuration = tickDuration;
			_simStartTime = simStartTime;
		}
		
		@Override
		public void onPreStart(StateData stateData) {
		}
		
		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<ServiceState> createService(
				int serviceId, StateData initData, ServiceContext<ServiceState> context,
				ConcentusHandle handle, MetricContext metricContext,
				ComponentResolver<TBuffer> resolver) {
			return new TickTimerService<>(_tickDuration, _simStartTime, handle, metricContext, resolver);
		}
	
	}
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final ConcentusHandle _concentusHandle;
	private final long _tickDuration;
	private final long _simStartTime;
	
	private final ZmqSocketManager<TBuffer> _socketManager;
	
	// tick publishing
	private final EventQueue<TBuffer> _tickPubEventQueue;
	private final SendQueue<OutgoingEventHeader, TBuffer> _tickPubSendQueue;
	private final OutgoingEventHeader _tickSendHeader = new OutgoingEventHeader(0, 2);
	private int _tickPubSocketId;
	
	// canonical state listening
	private final EventQueue<TBuffer> _canonicalStateRecvQueue;
	private final IncomingEventHeader _canonicalStateRecvHeader = new IncomingEventHeader(0, 1);
	private int _canonicalStateSubSocketId;
	
	private ProcessingPipeline<TBuffer> _tickPipeline;
	
	public TickTimerService(
			long tickDuration,
			long simStartTime,
			ConcentusHandle concentusHandle, 
			MetricContext metricContext, 
			ComponentResolver<TBuffer> resolver) {
		_tickDuration = tickDuration;
		_simStartTime = simStartTime;
		
		_concentusHandle = Objects.requireNonNull(concentusHandle);
		_socketManager = resolver.newSocketManager(concentusHandle.getClock());
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager
				.newMessageQueueFactory(resolver.getEventQueueFactory());
		
		_tickPubEventQueue = messageQueueFactory
				.createSingleProducerQueue("tickPubQueue", 16, 32, new BlockingWaitStrategy());
		_tickPubSendQueue = new SendQueue<OutgoingEventHeader, TBuffer>("tickPubQueue", _tickSendHeader, _tickPubEventQueue);
		
		_canonicalStateRecvQueue = messageQueueFactory.createSingleProducerQueue("canonicalStateRecvQueue", 2048, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, new BlockingWaitStrategy());
	}
	
	@Override
	protected void onConnect(StateData stateData, ClusterHandle cluster)
			throws Exception {
		_tickPubSocketId = _socketManager.create(ZMQ.PUB, "tickPub");
		List<ServiceEndpoint> actionCollectorEndpoints = 
				cluster.getAllServiceEndpoints(ConcentusEndpoints.ACTION_COLLECTOR_TICK_SUB.getId());
		for (ServiceEndpoint endpoint : actionCollectorEndpoints) {
			_socketManager.connectSocket(_tickPubSocketId, endpoint);
		}
		
		SocketSettings canonicalStateSubSocketSettings = SocketSettings.create()
				.subscribeTo(DataType.CANONICAL_STATE_UPDATE);
		_canonicalStateSubSocketId = _socketManager.create(ZMQ.SUB, canonicalStateSubSocketSettings, "canonicalStateSub");
		for (ServiceEndpoint endpoint : cluster.getAllServiceEndpoints(ConcentusEndpoints.CANONICAL_STATE_PUB.getId())) {
			_socketManager.connectSocket(_canonicalStateSubSocketId, endpoint);
		}
	}

	@Override
	protected void onStart(StateData stateData, ClusterHandle cluster)
			throws Exception {	
		EventListener<TBuffer> updateListener = new EventListener<>("cqnonicalStateListener", _canonicalStateRecvHeader, 
				_socketManager.getSocketMutex(_canonicalStateSubSocketId), _canonicalStateRecvQueue, _concentusHandle);
		
		TickTimerProcessor<TBuffer> tickTimerProcessor = new TickTimerProcessor<>(_tickDuration, _simStartTime, 
				_concentusHandle.getClock(), _canonicalStateRecvHeader, _tickPubSendQueue);
		
		_tickPipeline = ProcessingPipeline.<TBuffer>build(updateListener, _concentusHandle.getClock())
				.thenConnector(_canonicalStateRecvQueue)
				.then(_canonicalStateRecvQueue.createEventProcessor("TickTimerProcessor", tickTimerProcessor, 
						_concentusHandle.getClock(), _concentusHandle))
				.thenConnector(_tickPubEventQueue)
				.then(MessagingUtil.asSocketOwner("tickPub", _tickPubEventQueue, new Publisher<TBuffer>(_tickSendHeader), 
						_socketManager.getSocketMutex(_tickPubSocketId)))
				.createPipeline(_executor);

		_tickPipeline.start();
	}

	@Override
	protected void onShutdown(StateData stateData, ClusterHandle cluster)
			throws Exception {
		_tickPipeline.halt(30, TimeUnit.SECONDS);
	}
	
}
