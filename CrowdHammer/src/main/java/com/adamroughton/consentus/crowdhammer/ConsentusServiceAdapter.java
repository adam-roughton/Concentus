package com.adamroughton.consentus.crowdhammer;

import java.net.InetAddress;
import java.util.Objects;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.ConsentusServiceState;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.config.Configuration;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;

public class ConsentusServiceAdapter implements CrowdHammerService {

	private final Class<? extends ConsentusService> _serviceClass;
	private Configuration _configuration;
	private ConsentusProcessCallback _processCallback;
	private InetAddress _networkAddress;
	
	private ConsentusService _currentInstance;
	
	public ConsentusServiceAdapter(final Class<? extends ConsentusService> consentusServiceClass) {
		_serviceClass = Objects.requireNonNull(consentusServiceClass);
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			Cluster cluster) throws Exception {
		if (newClusterState == CrowdHammerServiceState.INIT_TEST) {
			try {
				_currentInstance = _serviceClass.newInstance();
				_currentInstance.configure(_configuration, _processCallback, _networkAddress);
			} catch (InstantiationException | IllegalAccessException | SecurityException e) {
				throw new RuntimeException(String.format("Could not instantiate service class %1$s."), e);
			}
		}
		ConsentusServiceState consentusState = newClusterState.getEquivalentSUTState();
		if (consentusState != null) {
			_currentInstance.onStateChanged(consentusState, cluster);
		}
		if (newClusterState == CrowdHammerServiceState.TEAR_DOWN) {
			_currentInstance = null;
		}
	}

	@Override
	public Class<CrowdHammerServiceState> getStateValueClass() {
		return CrowdHammerServiceState.class;
	}

	@Override
	public void configure(CrowdHammerConfiguration config,
			ConsentusProcessCallback exHandler, 
			InetAddress networkAddress) {
		_configuration = config;
		_processCallback = exHandler;
		_networkAddress = networkAddress;
	}

	@Override
	public String name() {
		return String.format("CrowdHammer Wrapping '%s'.", _serviceClass.getName());
	}

}
