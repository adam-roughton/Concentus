package com.adamroughton.concentus;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.ZMQ;

import com.adamroughton.concentus.ConcentusProcessCallback;
import com.adamroughton.concentus.cluster.worker.Cluster;
import com.adamroughton.concentus.config.Configuration;
import com.adamroughton.concentus.messaging.SocketSettings;

public abstract class ConsentusServiceBase implements ConsentusService {

	private final ExecutorService _executor = Executors.newCachedThreadPool();
	
	private final ZMQ.Context _zmqContext;
	private final Map<ZMQ.Socket, SocketSettings> _socketSettingsLookup = new HashMap<>();
	private final List<ZMQ.Socket> _sockets = new ArrayList<>();
	
	private enum State {
		INIT,
		RUNNING,
		USED
	}
	private final AtomicReference<State> _state = new AtomicReference<>(State.INIT);
	
	private Configuration _config;
	private ConcentusProcessCallback _exCallback;
	private InetAddress _networkAddress;
	
	public ConsentusServiceBase() {
		_zmqContext = ZMQ.context(1);
	}
	
	@Override
	public void configure(Configuration config,
			ConcentusProcessCallback exHandler, InetAddress networkAddress) {
		if (!_state.compareAndSet(State.INIT, State.RUNNING)) {
			throw new IllegalStateException("Each service instance can only be used once.");
		}
		_config = Objects.requireNonNull(config);
		_exCallback = Objects.requireNonNull(exHandler);
		_networkAddress = Objects.requireNonNull(networkAddress);
	}
	
	@Override
	public void onStateChanged(ConsentusServiceState newClusterState,
			Cluster cluster) throws Exception {
		if (_state.get() != State.RUNNING) {
			throw new IllegalStateException("The service instance was not in the running state.");
		}
		try {
			switch (newClusterState) {
			case INIT:
				enterInitState();
				break;
			case BIND:
				enterBindState();
				break;
			case CONNECT:
				enterConnectState();
				break;
			case START:
				enterStateState();
				break;
			case SHUTDOWN:
				enterShutdownState();
			}
			cluster.signalReady();
		} catch (Exception e) {
			enterShutdownState();
			_exCallback.signalFatalException(e);
		}
	}

	@Override
	public Class<ConsentusServiceState> getStateValueClass() {
		return ConsentusServiceState.class;
	}
	
	protected void onConfigure() {
		
	}
	
	private void enterInitState() {
		
	}
	
	protected void onInit() {
		
	}
	
	private void enterBindState() {
		
	}
	
	protected String[] onBind() {
		return new String[0];
	}
	
	private void enterConnectState() {
		
	}
	
	protected void onConnect() {
		
	}
	
	private void enterStateState() {
		onStart();
	}
	
	protected void onStart() {
		
	}
	
	private void enterShutdownState() {
		if (_state.compareAndSet(State.RUNNING, State.USED)) {
			Exception exp = null;
			try {
				onShutdown();
			} catch (Exception e) {
				exp = e;
			}
			_executor.shutdownNow();
			try {
				_executor.awaitTermination(1, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			_zmqContext.term();
			if (exp != null) {
				_exCallback.signalFatalException(new RuntimeException("Exception while shutting down:", exp));
			}
		}
	}
	
	protected void onShutdown() {	
	}
	
}
