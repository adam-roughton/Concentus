package com.adamroughton.consentus.crowdhammer.worker;

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

public class WorkerService implements ConsentusService {
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _updateDisruptor;
	private Disruptor<byte[]> _outputDisruptor;
	
	private UpdateListener _updateListener;
	private LoadDriver _loadDriver;
	private UpdateProcessor _updateProcessor;
	private InputEventPublisher _inputEventPublisher;	
	private MetricPublisher _metricPublisher;
	
	private ZMQ.Context _zmqContext;

	@SuppressWarnings("unchecked")
	@Override
	public void start(Config config, ConsentusProcessCallback exHandler) {
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		
		_updateDisruptor = new Disruptor<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
			
		}, _executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		_updateDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Input Disruptor", exHandler));
		
		_outputDisruptor = new Disruptor<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
			
		}, _executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());	
		_outputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Output Disruptor", exHandler));
		
		_inputEventPublisher = new InputEventPublisher(_zmqContext, config);
		_outputDisruptor.handleEventsWith(_inputEventPublisher);
		
		_updateProcessor = new UpdateProcessor(_outputDisruptor.getRingBuffer());
		_metricPublisher = new MetricPublisher(_zmqContext, (TestConfig)config);
		_updateDisruptor.handleEventsWith(_updateProcessor)
						.then(_metricPublisher);
		
		_outputDisruptor.start();
		_updateDisruptor.start();
		
		_updateListener = new UpdateListener(_zmqContext, _updateDisruptor.getRingBuffer(), config, exHandler);
		_executor.submit(_updateListener);
		
		_loadDriver = new LoadDriver(_outputDisruptor.getRingBuffer(), exHandler, config);
		_executor.submit(_loadDriver);
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
		return "CrowdHammer Worker Service";
	}
	
}
