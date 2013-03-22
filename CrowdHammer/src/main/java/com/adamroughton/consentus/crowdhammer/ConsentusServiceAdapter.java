/*
 * Copyright 2013 Adam Roughton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.adamroughton.consentus.crowdhammer;

import java.net.InetAddress;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Logger;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusService;
import com.adamroughton.consentus.ConsentusServiceState;
import com.adamroughton.consentus.cluster.worker.Cluster;
import com.adamroughton.consentus.config.Configuration;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;

public class ConsentusServiceAdapter implements CrowdHammerService {

	private final Logger _log;
	
	private final Class<? extends ConsentusService> _serviceClass;
	private Configuration _configuration;
	private ConsentusProcessCallback _processCallback;
	private InetAddress _networkAddress;
	
	private ConsentusService _currentInstance;
	
	public ConsentusServiceAdapter(final Class<? extends ConsentusService> consentusServiceClass) {
		_serviceClass = Objects.requireNonNull(consentusServiceClass);
		_log = Logger.getLogger(String.format("CrowdHammer wrapping '%s'", _serviceClass.getName()));
	}

	@Override
	public void onStateChanged(CrowdHammerServiceState newClusterState,
			Cluster cluster) throws Exception {
		_log.info(String.format("Entering state %s", newClusterState.name()));
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
			_currentInstance.onStateChanged(consentusState, new SignalReadyDisabledCluster(cluster));
		}
		if (newClusterState == CrowdHammerServiceState.TEAR_DOWN) {
			_currentInstance = null;
		}
		_log.info("Signalling ready for next state");
		cluster.signalReady();
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
	
	private class SignalReadyDisabledCluster implements Cluster {

		private final Cluster _wrappedCluster;
		
		public SignalReadyDisabledCluster(final Cluster wrappedCluster) {
			_wrappedCluster = Objects.requireNonNull(wrappedCluster);
		}
		
		@Override
		public void registerService(String serviceType, String address) {
			_wrappedCluster.registerService(serviceType, address);
		}

		@Override
		public String getServiceAtRandom(String serviceType) {
			return _wrappedCluster.getServiceAtRandom(serviceType);
		}

		@Override
		public String[] getAllServices(String serviceType) {
			return _wrappedCluster.getAllServices(serviceType);
		}

		@Override
		public void requestAssignment(String serviceType, byte[] requestBytes) {
			_wrappedCluster.requestAssignment(serviceType, requestBytes);
		}

		@Override
		public byte[] getAssignment(String serviceType) {
			return _wrappedCluster.getAssignment(serviceType);
		}

		@Override
		public void deleteAssignmentRequest(String serviceType) {
			_wrappedCluster.deleteAssignmentRequest(serviceType);
		}

		@Override
		public void signalReady() {
			_log.info("Suppressing signal ready");
		}

		@Override
		public UUID getMyId() {
			return _wrappedCluster.getMyId();
		}
		
	}

}
