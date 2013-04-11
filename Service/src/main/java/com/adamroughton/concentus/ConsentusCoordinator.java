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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import com.adamroughton.concentus.ConcentusProcess;
import com.adamroughton.concentus.ConcentusProcessCallback;
import com.adamroughton.concentus.ConcentusProcessConfiguration;
import com.adamroughton.concentus.DefaultProcessCallback;
import com.adamroughton.concentus.Util;
import com.adamroughton.concentus.ConcentusProcessConfiguration.ClusterFactory;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.config.Configuration;
import com.esotericsoftware.minlog.Log;

import asg.cliche.Command;
import asg.cliche.ShellFactory;

public final class ConsentusCoordinator implements ConcentusProcess<CoordinatorClusterHandle, Configuration>,
		ClusterFactory<CoordinatorClusterHandle> {

	public static final String PROCESS_NAME = "Consentus Coordinator";
	
	private CoordinatorClusterHandle _cluster;
	
	@Command(name="quit")
	public void quit() {
		System.exit(0);
	}

	@Override
	public void configure(CoordinatorClusterHandle cluster, Configuration config,
			ConcentusProcessCallback exHandler, InetAddress networkAddress) {
		_cluster = cluster;
	}

	@Override
	public String name() {
		return PROCESS_NAME;
	}

	@Override
	public void execute() throws InterruptedException {
		System.out.println("Starting up");
		
		// set assignments, move to next phases
		
		try {
		ShellFactory.createConsoleShell("Type quit to exit.", "Consentus", this)
        	.commandLoop();
		} catch (IOException eIO) {
			Log.error("Error starting Consentus Coordinator Shell", eIO);
		}
	}

	@Override
	public CoordinatorClusterHandle createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConcentusProcessCallback callback) {
		return new CoordinatorClusterHandle(zooKeeperAddress, zooKeeperRoot, callback);
	}
	
	public static void main(String[] args) {
		ConsentusCoordinator coordinator = new ConsentusCoordinator();
		ConcentusProcessConfiguration<CoordinatorClusterHandle, Configuration> baseConfig = 
				new ConcentusProcessConfiguration<>(coordinator, Configuration.class, new DefaultProcessCallback());
		Map<String, String> cmdLineValues = Util.parseCommandLine(PROCESS_NAME, baseConfig, args);
		baseConfig.configure(coordinator, cmdLineValues);
		try {
			coordinator.execute();
		} catch (Exception e) {
			Log.error("Error while running:", e);
			System.exit(1);
		}
	}
	
}