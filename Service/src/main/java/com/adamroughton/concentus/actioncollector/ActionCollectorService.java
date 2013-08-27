package com.adamroughton.concentus.actioncollector;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ConcentusEndpoints;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.cluster.worker.SimpleClusterWorkerHandle;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.config.ConfigurationUtil;
import com.adamroughton.concentus.data.ResizingBuffer;
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
import com.adamroughton.concentus.model.CollectiveApplication;
import com.adamroughton.concentus.pipeline.PipelineSection;
import com.adamroughton.concentus.pipeline.ProcessingPipeline;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.Util;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.YieldingWaitStrategy;

/**
 * Service that handles collecting actions from 
 * @author Adam Roughton
 *
 */
public class ActionCollectorService<TBuffer extends ResizingBuffer> {

	public final static String SERVICE_TYPE = "ActionCollector";
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private final int _id;
	private final SimpleClusterWorkerHandle _zooKeeperHandle;
	private final SocketManager<TBuffer> _socketManager;
	private final ProcessingPipeline<TBuffer> _pipeline;
	private final String _address;	
	private final TickEvent _tickEvent = new TickEvent();
	private final SendQueue<OutgoingEventHeader, TBuffer> _tickSendQueue;
	
	public ActionCollectorService(ConcentusHandle<? extends Configuration, TBuffer> concentusHandle,
			TickDelegate tickDelegate,
			int id,
			long startTime,
			long tickDuration) {
		_socketManager = Objects.requireNonNull(concentusHandle.newSocketManager());
		_id = id;
		
		Configuration config = concentusHandle.getConfig();
		Clock clock = concentusHandle.getClock();
		
		String zooKeeperAddress = concentusHandle.getZooKeeperAddress();
		String zooKeeperRoot = config.getZooKeeper().getAppRoot();
		String hostAddress = concentusHandle.getNetworkAddress().getHostAddress();
		int port = ConfigurationUtil.getPort(config, SERVICE_TYPE, "input");
		
		int recvQueueLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "recv");
		int sendQueueLength = ConfigurationUtil.getMessageBufferSize(config, SERVICE_TYPE, "send");
		
		_zooKeeperHandle = new SimpleClusterWorkerHandle(zooKeeperAddress, zooKeeperRoot, UUID.randomUUID(), concentusHandle);
		try {
			_zooKeeperHandle.start();
		} catch (Exception e) {
			concentusHandle.signalFatalException(e);
		}
		
		SocketSettings routerSocketSettings = SocketSettings.create()
				.bindToPort(port)
				.bindToInprocName("actionCollector");
		int routerSocketId = _socketManager.create(ZMQ.ROUTER, routerSocketSettings, "routerRecv");
		int[] boundPorts = _socketManager.getBoundPorts(routerSocketId);
		
		_address = String.format("tcp://%s:%d", hostAddress, boundPorts[0]);
		
		_zooKeeperHandle.registerServiceEndpoint(ConcentusEndpoints.ACTION_PROCESSOR, hostAddress, boundPorts[0]);
		_zooKeeperHandle.registerAsActionProcessor(_id);
		
		int tickSocketId = _socketManager.create(ZMQ.DEALER, "tickSend");
		_socketManager.connectSocket(tickSocketId, String.format("inproc://actionCollector"));
		
		SocketSettings dealerSetSocketSettings = SocketSettings.create()
				.setRecvPairAddress(_address);
		int dealerSetSocketId = _socketManager.create(SocketManager.DEALER_SET, dealerSetSocketSettings, "dealerSetSend");
		
		MessageQueueFactory<TBuffer> messageQueueFactory = _socketManager.newMessageQueueFactory(concentusHandle.getEventQueueFactory());
		
		EventQueue<TBuffer> recvQueue = messageQueueFactory.createSingleProducerQueue(
				"clientRecvQueue", 
				recvQueueLength,
				Constants.DEFAULT_MSG_BUFFER_SIZE,
				new YieldingWaitStrategy());
		EventQueue<TBuffer> sendQueue = messageQueueFactory.createSingleProducerQueue(
				"clientSendQueue",
				sendQueueLength, 
				Constants.MSG_BUFFER_ENTRY_LENGTH, 
				new YieldingWaitStrategy());
		EventQueue<TBuffer> tickQueue = messageQueueFactory
				.createMultiProducerQueue("tickQueue", 4, 32, new BlockingWaitStrategy());
		
		IncomingEventHeader recvHeader = new IncomingEventHeader(0, 2);
		OutgoingEventHeader sendHeader = new OutgoingEventHeader(0, 2);
		
		Mutex<Messenger<TBuffer>> tickMessenger = _socketManager.getSocketMutex(tickSocketId);
		EventProcessor tickPublisher = MessagingUtil.asSocketOwner("tickPublisher", tickQueue, new Publisher<TBuffer>(sendHeader), tickMessenger);
		_tickSendQueue = new SendQueue<>("tickSendQueue", sendHeader, tickQueue);
		
		SendQueue<OutgoingEventHeader, TBuffer> sendQueueWrapper = new SendQueue<OutgoingEventHeader, TBuffer>("sendQueue", sendHeader, sendQueue);
		CollectiveApplication application = Util.newInstance(_zooKeeperHandle.getApplicationClass(), CollectiveApplication.class);
		ActionCollectorProcessor<TBuffer> processor = new ActionCollectorProcessor<>(recvHeader, application, tickDelegate, sendQueueWrapper, startTime, tickDuration);
		
		Mutex<Messenger<TBuffer>> dealerSetMessenger = _socketManager.getSocketMutex(dealerSetSocketId);
		EventProcessor dealerSetPublisher = MessagingUtil.asSocketOwner("dealerSetPub", sendQueue, new Publisher<TBuffer>(sendHeader), dealerSetMessenger);
		
		PipelineSection<TBuffer> tickSection = ProcessingPipeline.<TBuffer>build(tickPublisher, clock)
				.thenConnector(recvQueue)
				.asSection();
		_pipeline = ProcessingPipeline.<TBuffer>build(new EventListener<>("routerListener", recvHeader, 
						_socketManager.getSocketMutex(routerSocketId), recvQueue, concentusHandle), clock)
				.thenConnector(recvQueue)
				.join(tickSection)
				.into(recvQueue.createEventProcessor("actionProcessor", processor))
				.thenConnector(sendQueue)
				.then(dealerSetPublisher)
				.createPipeline(_executor);
		_pipeline.start();
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
	
	public void close() {
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
