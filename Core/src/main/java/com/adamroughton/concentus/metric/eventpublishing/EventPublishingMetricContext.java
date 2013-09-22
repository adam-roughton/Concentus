package com.adamroughton.concentus.metric.eventpublishing;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.Clock;
import com.adamroughton.concentus.ComponentResolver;
import com.adamroughton.concentus.Constants;
import com.adamroughton.concentus.cluster.worker.ClusterHandle;
import com.adamroughton.concentus.data.ResizingBuffer;
import com.adamroughton.concentus.data.cluster.kryo.MetricMetaData;
import com.adamroughton.concentus.data.cluster.kryo.ServiceEndpoint;
import com.adamroughton.concentus.disruptor.CollocatedBufferEventFactory;
import com.adamroughton.concentus.disruptor.EventQueue;
import com.adamroughton.concentus.disruptor.EventQueueFactory;
import com.adamroughton.concentus.messaging.MessagingUtil;
import com.adamroughton.concentus.messaging.Messenger;
import com.adamroughton.concentus.messaging.OutgoingEventHeader;
import com.adamroughton.concentus.messaging.Publisher;
import com.adamroughton.concentus.messaging.zmq.ZmqSocketManager;
import com.adamroughton.concentus.messaging.zmq.SocketSettings;
import com.adamroughton.concentus.metric.LongValueMetricPublisher;
import com.adamroughton.concentus.metric.MetricContextBase;
import com.adamroughton.concentus.metric.MetricPublisher;
import com.adamroughton.concentus.util.Mutex;
import com.adamroughton.concentus.util.RunningStats;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventProcessor;

public class EventPublishingMetricContext<TBuffer extends ResizingBuffer> extends MetricContextBase {

	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final ClusterHandle _clusterHandle;
	private final ZmqSocketManager<TBuffer> _metricSocketManager;
	
	private final EventQueue<TBuffer> _metricPubQueue;
	private final OutgoingEventHeader _metricPubHeader;
	
	private EventProcessor _metricPublisher;
	
	public EventPublishingMetricContext(
			ClusterHandle clusterHandle,
			long metricTickMillis,
			long metricBufferMillis,
			Clock clock,
			ComponentResolver<TBuffer> componentResolver) {
		super(metricTickMillis, metricBufferMillis, clock);
		
		_clusterHandle = Objects.requireNonNull(clusterHandle);		
		_metricSocketManager = componentResolver.newSocketManager(clock);
		
		EventQueueFactory eventQueueFactory = componentResolver.getEventQueueFactory();
		CollocatedBufferEventFactory<TBuffer> bufferFactory = new CollocatedBufferEventFactory<>(
				2048, _metricSocketManager.getBufferFactory(), Constants.DEFAULT_MSG_BUFFER_SIZE);
		
		_metricPubQueue = eventQueueFactory.createMultiProducerQueue("metricPubQueue", 
				bufferFactory, bufferFactory.getCount(), new BlockingWaitStrategy());
		_metricPubHeader = new OutgoingEventHeader(0, 2);
	}
	
	@Override
	public void start(int metricSourceId) {
		super.start(metricSourceId);
		
		SocketSettings metricSocketSettings = SocketSettings.create();
		int metricSocketId = _metricSocketManager.create(ZMQ.PUB, metricSocketSettings, "metricPub");
		
		// get metric listener address
		ServiceEndpoint metricCollectorEndpoint = _clusterHandle.getMetricCollectorEndpoint();
		String metricCollectorAddress = String.format("tcp://%s:%d", metricCollectorEndpoint.ipAddress(), 
				metricCollectorEndpoint.port());
		_metricSocketManager.connectSocket(metricSocketId, metricCollectorAddress);
		
		Mutex<Messenger<TBuffer>> pubSocketMessenger = _metricSocketManager.getSocketMutex(metricSocketId);
		Publisher<TBuffer> metricPublisher = new Publisher<>(_metricPubHeader);
		_metricPublisher = MessagingUtil.asSocketOwner("metricPublisher", _metricPubQueue, metricPublisher, pubSocketMessenger);
		
		_executor.submit(_metricPublisher);
	}
	
	@Override
	public void stop() {
		_executor.shutdown();
		super.stop();
	}

	@Override
	protected MetricPublisher<RunningStats> newStatsMetricPublisher(
			MetricMetaData metaData) {
		return new RunningStatsMetricEventQueuePublisher<>(metaData.getMetricSourceId(),
				metaData.getMetricName(), metaData.getMetricType(), _metricPubQueue, _metricPubHeader);
	}

	@Override
	protected LongValueMetricPublisher newCountMetricPublisher(
			MetricMetaData metaData) {
		return new LongMetricEventQueuePublisher<>(metaData.getMetricSourceId(), 
				metaData.getMetricName(), metaData.getMetricType(), _metricPubQueue, _metricPubHeader);
	}

	@Override
	protected void onNewMetric(MetricMetaData metaData) {
		// push metric to cluster
		_clusterHandle.registerMetric(metaData);
	}
	
}
