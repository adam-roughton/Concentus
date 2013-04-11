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
package com.adamroughton.concentus;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.adamroughton.concentus.ConcentusProcess;
import com.adamroughton.concentus.ConcentusProcessCallback;
import com.adamroughton.concentus.ConcentusProcessConfiguration;
import com.adamroughton.concentus.ConsentusService;
import com.adamroughton.concentus.DefaultProcessCallback;
import com.adamroughton.concentus.Util;
import com.adamroughton.concentus.ConcentusProcessConfiguration.ClusterFactory;
import com.adamroughton.concentus.cluster.worker.WorkerClusterHandle;
import com.adamroughton.concentus.config.Configuration;
import com.esotericsoftware.minlog.Log;

public final class ConsentusWorker implements ConcentusProcess<WorkerClusterHandle, Configuration>, 
		ClusterFactory<WorkerClusterHandle> {

	public static final String SERVICE_CLASS_OPTION = "s";
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private ConsentusService _service;
	private WorkerClusterHandle _cluster;
	
	public ConsentusWorker(final ConsentusService service) {
		_service = Objects.requireNonNull(service);
	}

	@Override
	public void configure(WorkerClusterHandle cluster, Configuration config,
			ConcentusProcessCallback exHandler, InetAddress networkAddress) {
		_cluster = cluster;
		_service.configure(config, exHandler, networkAddress);
	}

	@Override
	public String name() {
		return _service.name();
	}

	@Override
	public void execute() throws InterruptedException {
		try (WorkerClusterHandle cluster = _cluster) {
			cluster.start();
			
			// Wait for exit
			Object waitMonitor = new Object();
			synchronized (waitMonitor) {
				waitMonitor.wait();
			}
			
		} catch (Exception e) {
			Log.error("Error thrown from the cluster participant", e);
		}
	}
	
	@Override
	public WorkerClusterHandle createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConcentusProcessCallback callback) {
		return new WorkerClusterHandle(zooKeeperAddress, zooKeeperRoot, _service, _executor, callback);
	}
	
	public static void main(String[] args) {
		Map<String, String> cmdLineValues = Util.parseCommandLine(
				"Consentus Worker", Arrays.asList(getServiceOption()), args, true);
		
		String serviceClassName = cmdLineValues.get(SERVICE_CLASS_OPTION);
		ConsentusService service = createConsentusService(serviceClassName);
		
		ConsentusWorker worker = new ConsentusWorker(service);
		
		ConcentusProcessConfiguration<WorkerClusterHandle, Configuration> baseConfig = 
				new ConcentusProcessConfiguration<>(worker, Configuration.class, new DefaultProcessCallback());
		
		cmdLineValues = Util.parseCommandLine(service.name(), baseConfig, args);
		baseConfig.configure(worker, cmdLineValues);
		try {
			worker.execute();
		} catch (Exception e) {
			Log.error("Error while running:", e);
			System.exit(1);
		}
	}
	
	@SuppressWarnings("unchecked")
	private static ConsentusService createConsentusService(final String serviceClassName) {
		try {
			Class<?> clazz = Class.forName(serviceClassName);
			if (ConsentusService.class.isAssignableFrom(clazz)) {
				Class<? extends ConsentusService> consentusServiceClass = 
						(Class<? extends ConsentusService>) clazz;
				return consentusServiceClass.newInstance();
			} else {
				throw new RuntimeException(String.format("The provided service class was not of type '%1$s'.", ConsentusService.class.getName()));
			}
		} catch (ClassNotFoundException eNotFound){
			throw new RuntimeException(String.format("Could not find the service class '%1$s'.", serviceClassName), eNotFound);
		} catch (InstantiationException | IllegalAccessException | SecurityException e) {
			throw new RuntimeException(String.format("Could not instantiate service class %1$s."), e);
		}
	}
	
	@SuppressWarnings("static-access")
	private static Option getServiceOption() {
		return OptionBuilder.withArgName("service class")
				.hasArgs()
				.isRequired(true)
				.withDescription("fully qualified class name of the service class.")
				.create(SERVICE_CLASS_OPTION);
	}
	
}
