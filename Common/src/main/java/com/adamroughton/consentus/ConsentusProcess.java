package com.adamroughton.consentus;

import java.net.InetAddress;

import com.adamroughton.consentus.cluster.ClusterParticipant;
import com.adamroughton.consentus.config.Configuration;

public interface ConsentusProcess<TCluster extends ClusterParticipant, 
		TConfig extends Configuration> {
	
	void configure(TCluster cluster, 
			TConfig config, 
			ConsentusProcessCallback exHandler, 
			InetAddress networkAddress);
	
	String name();
	
	/**
	 * Starts execution of the Consentus process. This call
	 * should block for the duration of execution. 
	 */
	void execute() throws InterruptedException;
	
}
