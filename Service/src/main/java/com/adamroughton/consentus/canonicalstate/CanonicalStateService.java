package com.adamroughton.consentus.canonicalstate;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.disruptor.FailFastExceptionHandler;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

import org.zeromq.*;

public class CanonicalStateService implements ConsentusService {
	
	private ExecutorService _executor;
	private Disruptor<byte[]> _inputDisruptor;
	private Disruptor<byte[]> _outputDisruptor;
	
	private EventListener _eventListener;
	private StateProcessor _stateProcessor;
	private Publisher _publisher;	
	
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
		
		_outputDisruptor = new Disruptor<>(new EventFactory<byte[]>() {

			@Override
			public byte[] newInstance() {
				return new byte[256];
			}
			
		}, _executor, new SingleThreadedClaimStrategy(2048), new YieldingWaitStrategy());
		_outputDisruptor.handleExceptionsWith(new FailFastExceptionHandler("Output Disruptor", exHandler));
		
		StateLogic testLogic = new StateLogic() {

			private int i = 0;
			
			@Override
			public void collectInput(ByteBuffer inputBuffer) {
				i++;
			}

			@Override
			public void tick(long simTime, long timeDelta) {
				i += 2;
			}

			@Override
			public void createUpdate(ByteBuffer updateBuffer) {
				updateBuffer.putInt(i);
			}
			
		};
		
		SequenceBarrier stateProcBarrier = _inputDisruptor.getRingBuffer().newBarrier();
		_stateProcessor = new StateProcessor(testLogic, _inputDisruptor.getRingBuffer(), 
				_outputDisruptor.getRingBuffer(), stateProcBarrier, exHandler, config);
		_inputDisruptor.handleEventsWith(_stateProcessor);
		
		_publisher = new Publisher(_zmqContext, config);
		_outputDisruptor.handleEventsWith(_publisher);
		
		_outputDisruptor.start();
		_inputDisruptor.start();
		
		_eventListener = new EventListener(_zmqContext, _inputDisruptor.getRingBuffer(), config, exHandler);
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
		return "Canonical State Service";
	}
	
}
