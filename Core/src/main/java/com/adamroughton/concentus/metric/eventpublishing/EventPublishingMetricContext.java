package com.adamroughton.concentus.metric.eventpublishing;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.FatalExceptionCallback;
import com.adamroughton.concentus.disruptor.EventEntryHandler;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.IncomingEventHeader;
import com.adamroughton.concentus.messaging.MessageBytesUtil;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.zmq.SocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketManagerImpl;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.LongValueMetricPublisher;
import com.adamroughton.concentus.metric.MetricContextBase;
import com.adamroughton.concentus.metric.MetricMetaData;
import com.adamroughton.concentus.metric.MetricPublisher;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.RunningStats;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventProcessor;

public class EventPublishingMetricContext extends MetricContextBase {

	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final SocketManager _metricSocketManager;
	
	private final EventQueue<byte[]> _metricPubQueue;
	private final OutgoingEventHeader _metricPubHeader;
	private final FatalExceptionCallback _exCallback;
	
	private EventProcessor _metricPublisher;
	private MetricMetaDataRequestListener _metaDataRequestListener;
	
	public EventPublishingMetricContext(
			long metricTickMillis,
			long metricBufferMillis,
			Clock clock,
			UUID sourceId,
			SocketSettings metricPubSocketSettings, 
			SocketSettings metaDataReqSocketSettings,
			EventQueueFactory eventQueueFactory,
			FatalExceptionCallback exCallback) {
		super(metricTickMillis, metricBufferMillis, clock);
		
		_metricPubQueue = eventQueueFactory.createMultiProducerQueue("metricPubQueue", 
				new EventEntryHandler<byte[]>() {

					@Override
					public byte[] newInstance() {
						return new byte[Constants.DEFAULT_MSG_BUFFER_SIZE];
					}

					@Override
					public void clear(byte[] event) {
						MessageBytesUtil.clear(event, 0, event.length);
					}

					@Override
					public void copy(byte[] source, byte[] destination) {
						System.arraycopy(source, 0, destination, 0, destination.length);
					}
				}, 2048, new BlockingWaitStrategy());
		_metricPubHeader = new OutgoingEventHeader(0, 2);
		_metricSocketManager = new SocketManagerImpl(clock);
		_exCallback = Objects.requireNonNull(exCallback);
		
		int metricPubSocketId = _metricSocketManager.create(ZMQ.PUB, metricPubSocketSettings, "metricPub");
		Mutex<Messenger> pubSocketMessenger = _metricSocketManager.getSocketMutex(metricPubSocketId);
		Publisher metricPublisher = new Publisher(_metricPubHeader);
		_metricPublisher = MessagingUtil.asSocketOwner("metricPublisher", _metricPubQueue, metricPublisher, pubSocketMessenger);
		
		int metaDataRouterSocketId = _metricSocketManager.create(ZMQ.ROUTER, metaDataReqSocketSettings, "metricMetaData");
		Mutex<Messenger> routerSocketMessenger = _metricSocketManager.getSocketMutex(metaDataRouterSocketId);
		IncomingEventHeader routerRecvHeader = new IncomingEventHeader(0, 2);
		OutgoingEventHeader routerSendHeader = new OutgoingEventHeader(0, 2);
		_metaDataRequestListener = new MetricMetaDataRequestListener(routerSocketMessenger, routerRecvHeader, routerSendHeader, sourceId, _exCallback);
	}
	
	@Override
	public void start() {
		_executor.submit(_metricPublisher);
		_executor.submit(_metaDataRequestListener);
	}
	
	@Override
	public void halt() {
		_metaDataRequestListener.halt();
	}

	@Override
	protected MetricPublisher<RunningStats> newStatsMetricPublisher(
			MetricMetaData metaData) {
		return new RunningStatsMetricEventQueuePublisher(metaData.getMetricName(), metaData.getMetricType(), _metricPubQueue, _metricPubHeader);
	}

	@Override
	protected LongValueMetricPublisher newCountMetricPublisher(
			MetricMetaData metaData) {
		return new LongMetricEventQueuePublisher(metaData.getMetricName(), metaData.getMetricType(), _metricPubQueue, _metricPubHeader);
	}

	@Override
	protected void onNewMetric(MetricMetaData metaData) {
		_metaDataRequestListener.putMetaData(metaData);
		super.onNewMetric(metaData);
	}
	
}
