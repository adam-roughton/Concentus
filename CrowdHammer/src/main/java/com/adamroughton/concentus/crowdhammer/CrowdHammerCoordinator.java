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
package com.adamroughton.concentus.crowdhammer;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.adamroughton.concentus.ConcentusProcess;
import com.adamroughton.concentus.ConcentusProcessCallback;
import com.adamroughton.concentus.ConcentusProcessConfiguration;
import com.adamroughton.concentus.DefaultProcessCallback;
import com.adamroughton.concentus.Util;
import com.adamroughton.concentus.ConcentusProcessConfiguration.ClusterFactory;
import com.adamroughton.concentus.cluster.coordinator.Cluster;
import com.adamroughton.concentus.cluster.coordinator.CoordinatorClusterHandle;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.esotericsoftware.minlog.Log;

import asg.cliche.Command;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;

public final class CrowdHammerCoordinator implements ConcentusProcess<CoordinatorClusterHandle, CrowdHammerConfiguration>, 
		ClusterFactory<CoordinatorClusterHandle> {

	public static final String PROCESS_NAME = "CrowdHammer Coordinator";
	private final static Logger LOG = Logger.getLogger("CrowdHammerCoordinator");
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private Future<?> _currentTask = null;
	private boolean _hasInitdTestInfrastructure = false;
	private ParticipatingNodes _participatingNodes = null;
	
	private CrowdHammerConfiguration _config;
	private CoordinatorClusterHandle _cluster;

	@Command(name="start")
	public void startRun(int... simClientCounts) throws Exception {
		if (!_hasInitdTestInfrastructure) {
			_participatingNodes = _cluster.getNodeSnapshot();
			System.out.println("Initialising test infrastructure");
			setAndWait(CrowdHammerServiceState.BIND, _participatingNodes, _cluster);
			setAndWait(CrowdHammerServiceState.CONNECT, _participatingNodes, _cluster);
			_hasInitdTestInfrastructure = true;
		}
		
		System.out.print("Starting test run with client counts: [");
		for (int i = 0; i < simClientCounts.length; i++) {
			if (i > 0) System.out.print(", ");
			System.out.print(simClientCounts[i]);
		}
		System.out.print("]\n");
		
		clearExisting();
		int testExecDur = _config.getCrowdHammer().getTestRunDurationInSeconds();
		
		TestRunner runner = new TestRunner(_participatingNodes, _cluster, testExecDur, simClientCounts);
		_currentTask = _executor.submit(runner);
	}
	
	@Command(name="stop")
	public void stopRun() {
		clearExisting();
	}
	
	@Command(name="quit")
	public void quit() {
		System.exit(0);
	}
	
	@Command(name="help")
	public void help() {
		System.out.println("Type start [counts] to begin a run, stop to cancel any " +
					"existing runs, and quit to exit.");
	}
	
	private void clearExisting() {
		if (_currentTask != null) {
			System.out.println("Stopping existing run");
			_currentTask.cancel(true);
			_currentTask = null;
		}
	}

	@Override
	public void configure(CoordinatorClusterHandle cluster,
			CrowdHammerConfiguration config,
			ConcentusProcessCallback exHandler, InetAddress networkAddress) {
		_cluster = cluster;
		_config = config;
	}

	@Override
	public String name() {
		return PROCESS_NAME;
	}

	@Override
	public void execute() throws InterruptedException {
		try {
			System.out.println("Starting CrowdHammer Coordinator");
			System.out.println("Connecting to ZooKeeper server");
			_cluster.start();
			_cluster.setState(CrowdHammerServiceState.INIT);
			Shell shell = ShellFactory.createConsoleShell(">", "CrowdHammer", this);
			help();
			shell.commandLoop();
		} catch (Exception e) {
			Log.error("Error creating console shell for " + PROCESS_NAME, e);
		}
	}

	@Override
	public CoordinatorClusterHandle createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConcentusProcessCallback callback) {
		return new CoordinatorClusterHandle(zooKeeperAddress, zooKeeperRoot, callback);
	}
	
	public static void main(String[] args) {
		CrowdHammerCoordinator coordinator = new CrowdHammerCoordinator();
		ConcentusProcessConfiguration<CoordinatorClusterHandle, CrowdHammerConfiguration> baseConfig = 
				new ConcentusProcessConfiguration<>(
						coordinator, 
						CrowdHammerConfiguration.class, 
						new DefaultProcessCallback());
		
		Map<String, String> commandLine = Util.parseCommandLine(PROCESS_NAME, baseConfig, args);
		baseConfig.configure(coordinator, commandLine);
		try {
			coordinator.execute();
		} catch (Exception e) {
			Log.error("Error while running:", e);
			System.exit(1);
		}
	}
	
	public static void setAndWait(CrowdHammerServiceState newState, ParticipatingNodes participatingNodes, Cluster cluster) 
			throws InterruptedException {
		LOG.info(String.format("Setting state to %s", newState.name()));
		cluster.setState(newState);
		LOG.info("Waiting for nodes...");
		while (!cluster.waitForReady(participatingNodes));
		LOG.info("All nodes ready, proceeding...");
	}
	
}
