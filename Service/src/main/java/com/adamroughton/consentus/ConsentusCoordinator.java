package com.adamroughton.consentus;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.ConsentusProcessConfiguration.ClusterFactory;
import com.adamroughton.consentus.DefaultProcessCallback;
import com.adamroughton.consentus.cluster.coordinator.ClusterCoordinator;
import com.adamroughton.consentus.config.Configuration;
import com.esotericsoftware.minlog.Log;

import asg.cliche.Command;
import asg.cliche.ShellFactory;

public final class ConsentusCoordinator implements ConsentusProcess<ClusterCoordinator, Configuration>,
		ClusterFactory<ClusterCoordinator> {

	public static final String PROCESS_NAME = "Consentus Coordinator";
	
	private ClusterCoordinator _cluster;
	
	@Command(name="quit")
	public void quit() {
		System.exit(0);
	}

	@Override
	public void configure(ClusterCoordinator cluster, Configuration config,
			ConsentusProcessCallback exHandler, InetAddress networkAddress) {
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
	public ClusterCoordinator createCluster(String zooKeeperAddress,
			String zooKeeperRoot, ConsentusProcessCallback callback) {
		return new ClusterCoordinator(zooKeeperAddress, zooKeeperRoot, callback);
	}
	
	public static void main(String[] args) {
		ConsentusCoordinator coordinator = new ConsentusCoordinator();
		ConsentusProcessConfiguration<ClusterCoordinator, Configuration> baseConfig = 
				new ConsentusProcessConfiguration<>(coordinator, Configuration.class, new DefaultProcessCallback());
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
