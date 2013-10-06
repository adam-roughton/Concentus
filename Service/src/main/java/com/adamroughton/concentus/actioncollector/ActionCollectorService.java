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
import com.adamroughton.concentus.data.DataType;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.data.cluster.kryo.ServiceInfo;
import com.adamroughton.concentus.data.cluster.kryo.ServiceState;
import com.adamroughton.concentus.data.events.bufferbacked.TickEvent;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.messaging.EventHeader;
import com.adamroughton.concentus.messaging.EventListener;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.MessengerBridge;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.patterns.EventPattern;
import com.adamroughton.concentus.messaging.patterns.EventWriter;
import com.adamroughton.concentus.messaging.patterns.SendQueue;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.MetricContext;
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Mutex.OwnerDelegate;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

public class ActionCollectorService<TBuffer extends ResizingBuffer> extends ConcentusServiceBase implements TickDriven {
	
	public static final ServiceInfo<ServiceState> SERVICE_INFO = new ServiceInfo<>(
					CoreServices.ACTION_PROCESSOR.getId(), 
					ServiceState.class, 
					CoreServices.CANONICAL_STATE.getId());
	
	public static class ActionCollectorServiceDeployment extends ServiceDeploymentBase<ServiceState> {

		private int _recvPort;
		private int _tickSubPort;
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
				int tickSubPort,
				int recvBufferLength,
				int sendBufferLength,
				TickDelegate tickDelegate,
				long startTime,
				long tickDuration) {
			super(SERVICE_INFO);
			_recvPort = recvPort;
			_tickSubPort = tickSubPort;
			_recvBufferLength = recvBufferLength;
			_sendBufferLength = sendBufferLength;
			_tickDelegate = Objects.requireNonNull(tickDelegate);
			_startTime = startTime;
			_tickDuration = tickDuration;
		}
		
		@Override
		public void onPreStart(StateData stateData) {
		}

		@Override
		public <TBuffer extends ResizingBuffer> ClusterService<ServiceState> createService(
				int serviceId, StateData initData, ServiceContext<ServiceState> context,
				ConcentusHandle handle, MetricContext metricContext,
				ComponentResolver<TBuffer> resolver) {
			return new ActionCollectorService<>(serviceId, _recvPort, _tickSubPort, _recvBufferLength, _sendBufferLength, 
					handle, metricContext, resolver, _tickDelegate, _startTime, _tickDuration);
		}
		
	}
	
	private final int _actionCollectorId;
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final ZmqSocketManager<TBuffer> _socketManager;
	private final ConcentusHandle _concentusHandle;
	private final TickDelegate _tickDelegate;
	private final long _startTime;
	private final long _tickDuration;
	
	private final IncomingEventHeader _recvHeader;
	private final OutgoingEventHeader _sendHeader;
	
	private final EventQueue<TBuffer> _recvQueue;
	private final EventQueue<TBuffer> _sendQueue;
	
	private final SocketSettings _routerSocketSettings;
	private final SocketSettings _tickSubSocketSettings;
	
	private Mutex<Messenger<TBuffer>> _internalTickMessenger;
	private final TBuffer _internalTickBuffer;
	private final TickEvent _tickEvent = new TickEvent();
	private final OutgoingEventHeader _internalTickHeader = new OutgoingEventHeader(0, 1);
	
	private ProcessingPipeline<TBuffer> _pipeline;
	private CollectiveApplication _application;
	
	public ActionCollectorService(
			int serviceId,
			int recvPort,
			int tickSubPort,
			int recvBufferLength,
			int sendBufferLength,
			ConcentusHandle concentusHandle,
			MetricContext metricContext,
			ComponentResolver<TBuffer> resolver,
			TickDelegate tickDelegate,
			long startTime,
			long tickDuration) {
		_actionCollectorId = serviceId;
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
		_tickSubSocketSettings = SocketSettings.create()
				.subscribeTo(DataType.TICK_EVENT)
				.bindToPort(tickSubPort);
		
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
		_internalTickBuffer = _socketManager.getBufferFactory().newInstance(32);
	}

	public void tick(final long time) {
		_internalTickMessenger.runAsOwner(new OwnerDelegate<Messenger<TBuffer>>() {

			@Override
			public void asOwner(Messenger<TBuffer> messenger) {
				EventPattern.writeContent(_internalTickBuffer, _internalTickHeader.getEventOffset(), 
						_internalTickHeader, _tickEvent, new EventWriter<OutgoingEventHeader, TickEvent>() {

					@Override
					public void write(OutgoingEventHeader header,
							TickEvent event) throws Exception {
						event.setTime(time);
					}
				});
				_sendHeader.setIsValid(_internalTickBuffer, true);
				messenger.send(_internalTickBuffer, _internalTickHeader, true);
			}
			
		});
	}
	
	@Override
	protected void onInit(StateData stateData,
			ClusterHandle cluster) throws Exception {
		_application = cluster.getApplicationInstanceFactory().newInstance();
	}

	@Override
	protected void onBind(StateData stateData,
			ClusterHandle cluster) throws Exception {
		int routerSocketId = _socketManager.create(ZMQ.ROUTER, _routerSocketSettings, "routerRecv");
		int recvPort = _socketManager.getBoundPort(routerSocketId);
		
		String address = String.format("tcp://%s:%d", 
				_concentusHandle.getNetworkAddress().getHostAddress(), 
				recvPort);
		
		SocketSettings dealerSetSocketSettings = SocketSettings.create()
				.setRecvPairAddress(address);
		int dealerSetSocketId = _socketManager.create(ZmqSocketManager.DEALER_SET, 
				dealerSetSocketSettings, "dealerSetSend");
		
		int internalTickSocketId = _socketManager.create(ZMQ.DEALER, "internalTick");
		_socketManager.connectSocket(internalTickSocketId, "inproc://actionCollector");
		_internalTickMessenger = _socketManager.getSocketMutex(internalTickSocketId);
		
		int tickSubSocketId = _socketManager.create(ZMQ.SUB, _tickSubSocketSettings, "tickSub");
		int tickSubPort = _socketManager.getBoundPort(tickSubSocketId);
		int tickBridgeSocketId = _socketManager.create(ZMQ.DEALER, "tickRouterBridge");
		_socketManager.connectSocket(tickBridgeSocketId, "inproc://actionCollector");
		
		/* forward tick events from the sub socket to the router socket. This allows the hot path of incoming
		 * action events to be processed as fast as possible (single producer onto the recv queue),
		 * with only a small cost to infrequent tick events
		 */
		MessengerBridge.BridgeDelegate<TBuffer> tickBridgeDelegate = new MessengerBridge.BridgeDelegate<TBuffer>() {

			@Override
			public void onMessageReceived(TBuffer recvBuffer,
					TBuffer sendBuffer, Messenger<TBuffer> sendMessenger,
					IncomingEventHeader recvHeader,
					OutgoingEventHeader sendHeader) {
				if (!EventHeader.isValid(recvBuffer, 0)) return;
				
				int contentSegmentMetaData = recvHeader.getSegmentMetaData(recvBuffer, 0);
				int contentOffset = EventHeader.getSegmentOffset(contentSegmentMetaData);
				int contentLength = EventHeader.getSegmentLength(contentSegmentMetaData);
				
				int sendContentOffset = sendHeader.getEventOffset();
				recvBuffer.copyTo(sendBuffer, contentOffset, sendContentOffset, contentLength);
				sendHeader.setSegmentMetaData(sendBuffer, 0, sendContentOffset, contentLength);
				sendHeader.setIsValid(sendBuffer, true);
				
				sendMessenger.send(sendBuffer, sendHeader, true);
			}
		};
		MessengerBridge<TBuffer> tickRouterBridge = _socketManager.newBridge(tickSubSocketId, tickBridgeSocketId, 
				tickBridgeDelegate, new IncomingEventHeader(0, 1), new OutgoingEventHeader(0, 1));
		
		ServiceEndpoint tickSubEndpoint = new ServiceEndpoint(_actionCollectorId, ConcentusEndpoints.ACTION_COLLECTOR_TICK_SUB.getId(), 
				_concentusHandle.getNetworkAddress().getHostAddress(), tickSubPort);
		cluster.registerServiceEndpoint(tickSubEndpoint);
		
		SendQueue<OutgoingEventHeader, TBuffer> sendQueueWrapper = 
				new SendQueue<OutgoingEventHeader, TBuffer>("sendQueue", _sendHeader, _sendQueue);
		
		ActionCollectorProcessor<TBuffer> processor = new ActionCollectorProcessor<>(_actionCollectorId, _recvHeader, 
				_application, _tickDelegate, sendQueueWrapper, _startTime, _tickDuration);
		
		Mutex<Messenger<TBuffer>> dealerSetMessenger = _socketManager.getSocketMutex(dealerSetSocketId);
		EventProcessor dealerSetPublisher = MessagingUtil.asSocketOwner("dealerSetPub", _sendQueue, 
				new Publisher<TBuffer>(_sendHeader), dealerSetMessenger);
		
		PipelineSection<TBuffer> tickSection = ProcessingPipeline.<TBuffer>build(tickRouterBridge, _concentusHandle.getClock())
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
		
		ServiceEndpoint endpoint = new ServiceEndpoint(_actionCollectorId, ConcentusEndpoints.ACTION_COLLECTOR.getId(), 
				_concentusHandle.getNetworkAddress().getHostAddress(), 
				recvPort);
		cluster.registerServiceEndpoint(endpoint);
	}

	@Override
	protected void onStart(StateData stateData,
			ClusterHandle cluster) throws Exception {
		_pipeline.start();
	}

	@Override
	protected void onShutdown(StateData stateData,
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
