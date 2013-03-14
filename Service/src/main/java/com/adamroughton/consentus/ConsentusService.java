package com.adamroughton.consentus;

import java.net.InetAddress;

import com.adamroughton.consentus.cluster.worker.ClusterListener;
import com.adamroughton.consentus.config.Configuration;

public interface ConsentusService extends ClusterListener<ConsentusServiceState> {

	void configure(Configuration config, ConsentusProcessCallback exHandler, InetAddress networkAddress);
	
	String name();
	
}
