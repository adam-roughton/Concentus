package com.adamroughton.concentus.actioncollector;

import java.io.IOException;
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
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.data.events.bufferbacked.TickEvent;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

public class ActionCollectorService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase {
	
	public static final ServiceInfo<ServiceState> SERVICE_INFO = new ServiceInfo<>(
					CoreServices.ACTION_PROCESSOR.getId(), 
					ServiceState.class, 
					CoreServices.CANONICAL_STATE.getId());
	
	public static class ActionCollectorServiceDeployment extends ServiceDeploymentBase<ServiceState> {

		private int _recvPort;
		private int _recvBufferLength;
		private int _sendBufferLength;
		private TickDelegate _tickDelegate;
		private long _startTime;
		private long _tickDuration;
		
		// for Kryo
		@SuppressWarnings("unused")
		private ActionCollectorServiceDeployment() {}
		
		public ActionCollectorServiceDeployment(
				int recvPort,
				int recvBufferLength,
				int sendBufferLength,
				TickDelegate tickDelegate,
				long startTime,
				long tickDuration) {
			super(SERVICE_INFO);
			_recvPort = recvPort;
			_recvBufferLength = recvBufferLength;
			_sendBufferLength = sendBufferLength;
			_tickDelegate = Objects.requireNonNull(tickDelegate);
			_startTime = startTime;
			_tickDuration = tickDuration;
		}
		
		@Override
		public void onPreStart(StateData<ServiceState> stateData) {
		}

		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<ServiceState> createService(
				int serviceId, ServiceContext<ServiceState> context,
				ConcentusHandle handle, MetricContext metricContext,
				ComponentResolver<TBuffer> resolver) {
			return new ActionCollectorService<>(serviceId, _recvPort, _recvBufferLength, _sendBufferLength, 
					handle, metricContext, resolver, _tickDelegate, _startTime, _tickDuration);
		}
		
	}
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final SocketManager<TBuffer> _socketManager;
	private final int _serviceId;
	private final ConcentusHandle _concentusHandle;
	private final TickDelegate _tickDelegate;
	private final long _startTime;
	private final long _tickDuration;
	
	private final IncomingEventHeader _recvHeader;
	private final OutgoingEventHeader _sendHeader;
	
	private final EventQueue<TBuffer> _recvQueue;
	private final EventQueue<TBuffer> _sendQueue;
	
	private final SocketSettings _routerSocketSettings;
	
	private final TickEvent _tickEvent = new TickEvent();
	private final SendQueue<OutgoingEventHeader, TBuffer> _tickSendQueue;
	private final EventProcessor _tickEventPublisher;
	
	private ProcessingPipeline<TBuffer> _pipeline;
	private CollectiveApplication _application;
	
	public ActionCollectorService(
			int serviceId,
			int recvPort,
			int recvBufferLength,
			int sendBufferLength,
			ConcentusHandle concentusHandle,
			MetricContext metricContext,
			ComponentResolver<TBuffer> resolver,
			TickDelegate tickDelegate,
			long startTime,
			long tickDuration) {
		_serviceId = serviceId;
		_concentusHandle = concentusHandle;
		_socketManager = Objects.requireNonNull(resolver.newSocketManager(concentusHandle.getClock()));
		_tickDelegate = Objects.requireNonNull(tickDelegate);
		_startTime = startTime;
		_tickDuration = tickDuration;
		
		_recvHeader = new IncomingEventHeader(0, 2);
		_sendHeader = new OutgoingEventHeader(0, 2);
		
		_routerSocketSettings = SocketSettings.create()
				.bindToPort(recvPort)
				.bindToInprocName("actionCollector");
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager
				.newMessageQueueFactory(resolver.getEventQueueFactory());
		
		_recvQueue = messageQueueFactory.createSingleProducerQueue(
				"clientRecvQueue", 
				recvBufferLength,
				Constants.DEFAULT_MSG_BUFFER_SIZE,
				new YieldingWaitStrategy());
		_sendQueue = messageQueueFactory.createSingleProducerQueue(
				"clientSendQueue",
				sendBufferLength, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		EventQueue<TBuffer> tickQueue = messageQueueFactory
				.createMultiProducerQueue("tickQueue", 4, 32, new BlockingWaitStrategy());
		
		int tickSocketId = _socketManager.create(ZMQ.DEALER, "tickSend");
		_socketManager.connectSocket(tickSocketId, String.format("inproc://actionCollector"));
		
		Mutex<Messenger<TBuffer>> tickMessenger = _socketManager.getSocketMutex(tickSocketId);
		_tickEventPublisher = MessagingUtil.asSocketOwner("tickPublisher", tickQueue, new Publisher<TBuffer>(_sendHeader), tickMessenger);
		_tickSendQueue = new SendQueue<>("tickSendQueue", _sendHeader, tickQueue);
	}
	
	public synchronized void tick(final long time) {
		_tickSendQueue.send(EventPattern.asTask(_tickEvent, new EventWriter<OutgoingEventHeader, TickEvent>() {

			@Override
			public void write(OutgoingEventHeader header, TickEvent event)
					throws Exception {
				event.setTime(time);
			}
		}));
	}

	@Override
	protected void onInit(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
		_application = cluster.getApplicationInstanceFactory().newInstance();
	}

	@Override
	protected void onBind(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
		int routerSocketId = _socketManager.create(ZMQ.ROUTER, _routerSocketSettings, "routerRecv");
		int[] boundPorts = _socketManager.getBoundPorts(routerSocketId);
		int recvPort = boundPorts[0];
		
		String address = String.format("tcp://%s:%d", 
				_concentusHandle.getNetworkAddress().getHostAddress(), 
				recvPort);
		
		SocketSettings dealerSetSocketSettings = SocketSettings.create()
				.setRecvPairAddress(address);
		int dealerSetSocketId = _socketManager.create(SocketManager.DEALER_SET, 
				dealerSetSocketSettings, "dealerSetSend");
		
		SendQueue<OutgoingEventHeader, TBuffer> sendQueueWrapper = 
				new SendQueue<OutgoingEventHeader, TBuffer>("sendQueue", _sendHeader, _sendQueue);
		
		ActionCollectorProcessor<TBuffer> processor = new ActionCollectorProcessor<>(_recvHeader, 
				_application, _tickDelegate, sendQueueWrapper, _startTime, _tickDuration);
		
		Mutex<Messenger<TBuffer>> dealerSetMessenger = _socketManager.getSocketMutex(dealerSetSocketId);
		EventProcessor dealerSetPublisher = MessagingUtil.asSocketOwner("dealerSetPub", _sendQueue, 
				new Publisher<TBuffer>(_sendHeader), dealerSetMessenger);
		
		PipelineSection<TBuffer> tickSection = ProcessingPipeline.<TBuffer>build(_tickEventPublisher, _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.asSection();
		_pipeline = ProcessingPipeline.<TBuffer>build(new EventListener<>("routerListener", _recvHeader, 
						_socketManager.getSocketMutex(routerSocketId), _recvQueue, _concentusHandle), _concentusHandle.getClock())
				.thenConnector(_recvQueue)
				.join(tickSection)
				.into(_recvQueue.createEventProcessor("actionProcessor", processor))
				.thenConnector(_sendQueue)
				.then(dealerSetPublisher)
				.createPipeline(_executor);
		
		ServiceEndpoint endpoint = new ServiceEndpoint(ConcentusEndpoints.ACTION_PROCESSOR.getId(), 
				_concentusHandle.getNetworkAddress().getHostAddress(), 
				recvPort);
		cluster.registerServiceEndpoint(endpoint);
	}

	@Override
	protected void onStart(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
		_pipeline.start();
	}

	@Override
	protected void onShutdown(StateData<ServiceState> stateData,
			ClusterHandle cluster) throws Exception {
		if (_pipeline != null) {
			try {
				_pipeline.halt(60, TimeUnit.SECONDS);
			} catch (InterruptedException eInterrupted) {
				throw new RuntimeException("Interrupted while waiting for pipeline to halt.");
			}
		}
		if (_socketManager != null) {
			try {
				_socketManager.close();
			} catch (IOException eIO) {
				throw new RuntimeException("Error while closing the socket manager", eIO);
			}
		}
	}
	
	
}
