package com.adamroughton.consentus.crowdhammer.metriclistener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.crowdhammer.TestConfig;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class MetricListenerService implements ConsentusService {
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _inputDisruptor;
	
	private EventListener _eventListener;
	private MetricProcessor _metricProcessor;
	
	private ZMQ.Context _zmqContext;

	@SuppressWarnings("unchecked")
	@Override
	public void start(Config config, ConsentusProcessCallback exHandler) {
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		
		_inputDisruptor = new Disruptor<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
			
		}, _executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		_inputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Input Disruptor", exHandler));
		
		_metricProcessor = new MetricProcessor();
		_inputDisruptor.handleEventsWith(_metricProcessor);
		
		_inputDisruptor.start();
		
		_eventListener = new EventListener(_zmqContext, _inputDisruptor.getRingBuffer(), (TestConfig)config, exHandler);
		_executor.submit(_eventListener);
	}
	
	@Override
	public void shutdown() {
		_zmqContext.term();
		_executor.shutdownNow();
		try {
			_executor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException eInterrupted) {
			// ignore
		}
	}

	@Override
	public String name() {
		return "Metric Listener Service";
	}
}
