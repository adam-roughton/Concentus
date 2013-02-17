package com.adamroughton.consentus.canonicalstate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.adamroughton.consentus.ConcentusService;
import com.adamroughton.consentus.Config;
import com.adamroughton.consentus.ConcentusProcessCallback;
import org.zeromq.*;

public class CanonicalStateService implements ConcentusService {
	
	private ExecutorService _executor;
	
	private EventListener _eventListener;
	private StateProcessor _stateProcessor;
	private Publisher _publisher;	
	
	private ZMQ.Context _zmqContext;

	@Override
	public void start(Config config, ConcentusProcessCallback exHandler) {
		_executor = Executors.newCachedThreadPool();
		_zmqContext = ZMQ.context(1);
		
		
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
