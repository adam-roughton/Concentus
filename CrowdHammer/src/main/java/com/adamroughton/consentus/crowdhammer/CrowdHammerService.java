package com.adamroughton.consentus.crowdhammer;

import java.net.InetAddress;

import com.adamroughton.consentus.ConsentusProcessCallback;
import com.adamroughton.consentus.cluster.worker.ClusterListener;
import com.adamroughton.consentus.crowdhammer.config.CrowdHammerConfiguration;

public interface CrowdHammerService extends ClusterListener<CrowdHammerServiceState> {

	void configure(CrowdHammerConfiguration config, ConsentusProcessCallback exHandler, InetAddress networkAddress);
	
	String name();
	
}
