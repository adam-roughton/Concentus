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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.adamroughton.concentus.ConcentusCoordinatorProcess;
import com.adamroughton.concentus.ConcentusExecutableOperations;
import com.adamroughton.concentus.ConcentusExecutableOperations.FactoryDelegate;
import com.adamroughton.concentus.ConcentusHandle;
import com.adamroughton.concentus.ConcentusNode;
import com.adamroughton.concentus.ConcentusProcessFactory;
import com.adamroughton.concentus.NoArgsConcentusProcessFactory;
import com.adamroughton.concentus.cluster.coordinator.ClusterCoordinatorHandle;
import com.adamroughton.concentus.cluster.coordinator.ParticipatingNodes;
import com.adamroughton.concentus.crowdhammer.config.CrowdHammerConfiguration;
import com.esotericsoftware.minlog.Log;

import asg.cliche.Command;
import asg.cliche.Shell;
import asg.cliche.ShellFactory;

public final class CrowdHammerCoordinatorNode implements ConcentusNode<ConcentusCoordinatorProcess, CrowdHammerConfiguration> {

	public static final String PROCESS_NAME = "CrowdHammer Coordinator";
	private final static Logger LOG = Logger.getLogger("CrowdHammerCoordinator");
	
	@Override
	public ConcentusProcessFactory<ConcentusCoordinatorProcess, CrowdHammerConfiguration> getProcessFactory() {
		return new NoArgsConcentusProcessFactory<>(PROCESS_NAME, 
			new FactoryDelegate<ConcentusCoordinatorProcess, CrowdHammerConfiguration>() {

				@Override
				public ConcentusCoordinatorProcess create(
						ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle) {
					return new CrowdHammerCoordinatorProcess(concentusHandle);
				}
			
			}, 
			ConcentusCoordinatorProcess.class,
			CrowdHammerConfiguration.class);
	}
	
	public static void main(String[] args) {
		ConcentusExecutableOperations.executeClusterCoordinator(args, new CrowdHammerCoordinatorNode());
	}
	
	public static void setAndWait(CrowdHammerServiceState newState, ParticipatingNodes participatingNodes, ClusterCoordinatorHandle cluster) 
			throws InterruptedException {
		LOG.info(String.format("Setting state to %s", newState.name()));
		cluster.setState(newState);
		LOG.info("Waiting for nodes...");
		while (!cluster.waitForReady(participatingNodes));
		LOG.info("All nodes ready, proceeding...");
	}
	
	public static class CrowdHammerCoordinatorProcess implements ConcentusCoordinatorProcess {
		private final ExecutorService _executor = Executors.newCachedThreadPool();
		private final ConcentusHandle<? extends CrowdHammerConfiguration> _concentusHandle;
		private ClusterCoordinatorHandle _clusterHandle;
		
		private Future<?> _currentTask = null;
		private boolean _hasInitdTestInfrastructure = false;
		private ParticipatingNodes _participatingNodes = null;
			
		public CrowdHammerCoordinatorProcess(ConcentusHandle<? extends CrowdHammerConfiguration> concentusHandle) {
			_concentusHandle = Objects.requireNonNull(concentusHandle);
		}
		
		@Command(name="start")
		public void startRun(int... simClientCounts) throws Exception {
			if (!_hasInitdTestInfrastructure) {
				_participatingNodes = _clusterHandle.getNodeSnapshot();
				System.out.println("Initialising test infrastructure");
				setAndWait(CrowdHammerServiceState.BIND, _participatingNodes, _clusterHandle);
				setAndWait(CrowdHammerServiceState.CONNECT, _participatingNodes, _clusterHandle);
				_hasInitdTestInfrastructure = true;
			}
			
			System.out.print("Starting test run with client counts: [");
			for (int i = 0; i < simClientCounts.length; i++) {
				if (i > 0) System.out.print(", ");
				System.out.print(simClientCounts[i]);
			}
			System.out.print("]\n");
			
			clearExisting();
			int testExecDur = _concentusHandle.getConfig().getCrowdHammer().getTestRunDurationInSeconds();
			
			TestRunner runner = new TestRunner(_participatingNodes, _clusterHandle, testExecDur, simClientCounts);
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
		public void run(ClusterCoordinatorHandle coordinatorHandle) {
			_clusterHandle = coordinatorHandle;
			try {
				System.out.println("Starting CrowdHammer Coordinator");
				System.out.println("Connecting to ZooKeeper server");
				coordinatorHandle.start();
				coordinatorHandle.setState(CrowdHammerServiceState.INIT);
				Shell shell = ShellFactory.createConsoleShell(">", "CrowdHammer", this);
				help();
				shell.commandLoop();
			} catch (Exception e) {
				Log.error("Error creating console shell for " + PROCESS_NAME, e);
			}
		}
		
		
	}
	
}
