package com.adamroughton.consentus.crowdhammer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.adamroughton.consentus.ConsentusProcess;
import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusProcessConfiguration;
import com.adamroughton.consentus.ConsentusProcessConfiguration.ClusterFactory;
import com.adamroughton.consentus.DefaultProcessCallback;
import com.adamroughton.consentus.Util;
import com.adamroughton.consentus.cluster.coordinator.ClusterCoordinator;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;
import com.esotericsoftware.minlog.Log;

import asg.cliche.Command;
import asg.cliche.ShellFactory;

public final class CrowdHammerCoordinator implements ConsentusProcess<ClusterCoordinator, CrowdHammerConfiguration>, 
		ClusterFactory<ClusterCoordinator> {

	public static final String PROCESS_NAME = "CrowdHammer Coordinator";
	
	private final ExecutorService _executor = Executors.newCachedThreadPool();
	private Future<?> _currentTask = null;
	
	private CrowdHammerConfiguration _config;
	private ClusterCoordinator _cluster;

	@Command(name="start")
	public void startRun(int... simClientCounts) {
		clearExisting();
		int testExecDur = _config.getCrowdHammer().getTestRunDurationInSeconds();
		TestRunner runner = new TestRunner(_cluster, testExecDur, simClientCounts);
		_executor.execute(runner);
	}
	
	@Command(name="stop")
	public void stopRun() {
		clearExisting();
	}
	
	@Command(name="quit")
	public void quit() {
		System.exit(0);
	}
	
	private void clearExisting() {
		if (_currentTask != null) {
			System.out.println("Stopping current run");
			_currentTask.cancel(true);
			_currentTask = null;
		}
	}

	@Override
	public void configure(ClusterCoordinator cluster,
			CrowdHammerConfiguration config,
			ConsentusProcessCallback exHandler, InetAddress networkAddress) {
		_cluster = cluster;
	}

	@Override
	public String name() {
		return PROCESS_NAME;
	}

	@Override
	public void execute() throws InterruptedException {
		try {
			ShellFactory.createConsoleShell("Type start [counts] to begin a run, stop to cancel any " +
					"existing runs, and quit to exit.", "CrowdHammer", this)
	        	.commandLoop();
		} catch (IOException eIO) {
			Log.error("Error creating console shell for " + PROCESS_NAME, eIO);
		}
	}

	@Override
	public ClusterCoordinator createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConsentusProcessCallback callback) {
		return new ClusterCoordinator(zooKeeperAddress, zooKeeperRoot, callback);
	}
	
	public static void main(String[] args) {
		CrowdHammerCoordinator coordinator = new CrowdHammerCoordinator();
		ConsentusProcessConfiguration<ClusterCoordinator, CrowdHammerConfiguration> baseConfig = 
				new ConsentusProcessConfiguration<>(
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
	
}
